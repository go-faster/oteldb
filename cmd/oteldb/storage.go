package main

import (
	"context"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/prometheus/prometheus/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelreceiver"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type otelStorage struct {
	logQuerier  logQuerier
	logInserter logstorage.Inserter

	traceQuerier  traceQuerier
	traceInserter tracestorage.Inserter

	metricsQuerier  storage.Queryable
	metricsConsumer otelreceiver.MetricsConsumer
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
