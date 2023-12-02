package main

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	ytzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"

	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/metricsharding"
	"github.com/go-faster/oteldb/internal/otelreceiver"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/tracestorage"
	"github.com/go-faster/oteldb/internal/yqlclient"
	"github.com/go-faster/oteldb/internal/ytstorage"
)

type combinedYTQuerier struct {
	yql *ytstorage.YQLQuerier
	*ytstorage.YTQLQuerier
}

func (q *combinedYTQuerier) Capabilities() (caps logqlengine.QuerierCapabilities) {
	return q.yql.Capabilities()
}

func (q *combinedYTQuerier) SelectLogs(ctx context.Context, start, end otelstorage.Timestamp, params logqlengine.SelectLogsParams) (_ iterators.Iterator[logstorage.Record], rerr error) {
	return q.yql.SelectLogs(ctx, start, end, params)
}

type otelStorage struct {
	logQuerier  logQuerier
	logInserter logstorage.Inserter

	traceQuerier  traceQuerier
	traceInserter tracestorage.Inserter

	metricsQuerier  storage.Queryable
	metricsConsumer otelreceiver.MetricsConsumer
}

func setupYT(ctx context.Context, lg *zap.Logger, m *app.Metrics) (otelStorage, error) {
	cfg := &yt.Config{
		Logger:                &ytzap.Logger{L: lg.Named("yc")},
		DisableProxyDiscovery: true,
	}
	clusterName, useYQL := os.LookupEnv("YT_YQL_CLUSTER")
	if useYQL {
		return setupYQL(ctx, lg, m, clusterName, cfg)
	}
	return setupYTQL(ctx, lg, m, cfg)
}

func setupYQL(ctx context.Context, lg *zap.Logger, m *app.Metrics, clusterName string, cfg *yt.Config) (otelStorage, error) {
	zctx.From(ctx).Info("Setting up YQL")
	yc, err := ythttp.NewClient(cfg)
	if err != nil {
		return otelStorage{}, errors.Wrap(err, "yt")
	}

	proxy, err := cfg.GetProxy()
	if err != nil {
		return otelStorage{}, errors.Wrap(err, "get proxy addr")
	}

	yqlClient, err := yqlclient.NewClient("http://"+proxy, yqlclient.ClientOptions{
		Token: cfg.GetToken(),
		Client: &http.Client{
			Transport: otelhttp.NewTransport(
				http.DefaultTransport,
				otelhttp.WithTracerProvider(m.TracerProvider()),
				otelhttp.WithMeterProvider(m.MeterProvider()),
			),
		},
		TracerProvider: m.TracerProvider(),
		MeterProvider:  m.MeterProvider(),
	})
	if err != nil {
		return otelStorage{}, errors.Wrap(err, "create YQL client")
	}

	tables := ytstorage.NewStaticTables(ypath.Path("//oteldb"))
	if err := migrateYT(ctx, yc, lg, tables); err != nil {
		return otelStorage{}, errors.Wrap(err, "migrate")
	}

	inserter, err := ytstorage.NewInserter(yc, ytstorage.InserterOptions{
		Tables:         tables,
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),
	})
	if err != nil {
		return otelStorage{}, errors.Wrap(err, "create inserter")
	}

	engineQuerier, err := ytstorage.NewYQLQuerier(yqlClient, ytstorage.YQLQuerierOptions{
		Tables:         tables,
		ClusterName:    clusterName,
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),
	})
	if err != nil {
		return otelStorage{}, errors.Wrap(err, "create engine querier")
	}

	labelQuerier, err := ytstorage.NewYTQLQuerier(yc, ytstorage.YTQLQuerierOptions{
		Tables:         tables,
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),
	})
	if err != nil {
		return otelStorage{}, errors.Wrap(err, "create label querier")
	}

	querier := &combinedYTQuerier{
		yql:         engineQuerier,
		YTQLQuerier: labelQuerier,
	}

	shardOpts := metricsharding.ShardingOptions{}
	metricShard := metricsharding.NewSharder(yc, yqlClient, shardOpts)
	metricConsumer := metricsharding.NewConsumer(yc, shardOpts)
	return otelStorage{
		logQuerier:      querier,
		logInserter:     inserter,
		traceQuerier:    querier,
		traceInserter:   inserter,
		metricsQuerier:  metricShard,
		metricsConsumer: metricConsumer,
	}, nil
}

func setupYTQL(ctx context.Context, lg *zap.Logger, m *app.Metrics, cfg *yt.Config) (otelStorage, error) {
	zctx.From(ctx).Info("Setting up YTQL")
	yc, err := ythttp.NewClient(cfg)
	if err != nil {
		return otelStorage{}, errors.Wrap(err, "yt")
	}

	tables := ytstorage.NewTables(ypath.Path("//oteldb"))
	if err := migrateYT(ctx, yc, lg, tables); err != nil {
		return otelStorage{}, errors.Wrap(err, "migrate")
	}

	inserter, err := ytstorage.NewInserter(yc, ytstorage.InserterOptions{
		Tables:         tables,
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),
	})
	if err != nil {
		return otelStorage{}, errors.Wrap(err, "create inserter")
	}

	querier, err := ytstorage.NewYTQLQuerier(yc, ytstorage.YTQLQuerierOptions{
		Tables:         tables,
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),
	})
	if err != nil {
		return otelStorage{}, errors.Wrap(err, "create querier")
	}

	return otelStorage{
		logQuerier:      querier,
		logInserter:     inserter,
		traceQuerier:    querier,
		traceInserter:   inserter,
		metricsQuerier:  nil,
		metricsConsumer: nil,
	}, nil
}

func migrateYT(ctx context.Context, yc yt.Client, lg *zap.Logger, tables ytstorage.Tables) error {
	migrateBackoff := backoff.NewExponentialBackOff()
	migrateBackoff.InitialInterval = 2 * time.Second
	migrateBackoff.MaxElapsedTime = time.Minute

	return backoff.Retry(func() error {
		err := tables.Migrate(ctx, yc, migrate.OnConflictTryAlter(ctx, yc))
		if err != nil {
			lg.Error("Migration failed", zap.Error(err))
			// FIXME(tdakkota): client does not return a proper error to check
			//  the error message and there is no specific ErrorCode for this error.
			if !strings.Contains(err.Error(), "no healthy tablet cells") {
				return backoff.Permanent(err)
			}
		}
		return err
	}, migrateBackoff)
}

func setupCH(
	ctx context.Context,
	dsn string,
	lg *zap.Logger,
	m *app.Metrics,
) (store otelStorage, _ error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return store, errors.Wrap(err, "parse DSN")
	}

	pass, _ := u.User.Password()
	chLogger := lg.Named("ch")
	{
		var lvl zapcore.Level
		if v := os.Getenv("CH_LOG_LEVEL"); v != "" {
			if err := lvl.UnmarshalText([]byte(v)); err != nil {
				return store, errors.Wrap(err, "parse log level")
			}
		} else {
			lvl = lg.Level()
		}
		chLogger = chLogger.WithOptions(zap.IncreaseLevel(lvl))
	}
	opts := ch.Options{
		Logger:         chLogger,
		Address:        u.Host,
		Database:       strings.TrimPrefix(u.Path, "/"),
		User:           u.User.Username(),
		Password:       pass,
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),

		// Capture query body and other parameters.
		OpenTelemetryInstrumentation: true,
	}

	// First thing that every Yandex employee do is forgetting how to setup
	// a docker liveness probe.
	connectBackoff := backoff.NewExponentialBackOff()
	connectBackoff.InitialInterval = 2 * time.Second
	connectBackoff.MaxElapsedTime = time.Minute
	c, err := backoff.RetryWithData(func() (*chpool.Pool, error) {
		c, err := chpool.Dial(ctx, chpool.Options{
			ClientOptions: opts,
		})
		if err != nil {
			return nil, errors.Wrap(err, "dial")
		}
		return c, nil
	}, connectBackoff)
	if err != nil {
		return store, errors.Wrap(err, "migrate")
	}

	tables := chstorage.DefaultTables()
	if err := tables.Create(ctx, c); err != nil {
		return store, errors.Wrap(err, "create tables")
	}

	inserter, err := chstorage.NewInserter(c, chstorage.InserterOptions{
		Tables:         tables,
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),
	})
	if err != nil {
		return store, errors.Wrap(err, "create inserter")
	}

	querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
		Tables:         tables,
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),
	})
	if err != nil {
		return store, errors.Wrap(err, "create querier")
	}

	return otelStorage{
		logQuerier:      querier,
		logInserter:     inserter,
		traceQuerier:    querier,
		traceInserter:   inserter,
		metricsQuerier:  querier,
		metricsConsumer: inserter,
	}, nil
}
