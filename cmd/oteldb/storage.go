package main

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	ytzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/ytstorage"
)

func setupYT(ctx context.Context, lg *zap.Logger) (*ytstorage.Inserter, *ytstorage.YTQLQuerier, error) {
	yc, err := ythttp.NewClient(&yt.Config{
		Logger: &ytzap.Logger{L: lg.Named("yc")},
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "yt")
	}

	tables := ytstorage.NewTables(ypath.Path("//oteldb"))
	{
		migrateBackoff := backoff.NewExponentialBackOff()
		migrateBackoff.InitialInterval = 2 * time.Second
		migrateBackoff.MaxElapsedTime = time.Minute

		if err := backoff.Retry(func() error {
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
		}, migrateBackoff); err != nil {
			return nil, nil, errors.Wrap(err, "migrate")
		}
	}

	inserter := ytstorage.NewInserter(yc, tables)
	querier := ytstorage.NewYTQLQuerier(yc, tables)
	return inserter, querier, nil
}

func setupCH(
	ctx context.Context,
	dsn string,
	lg *zap.Logger,
	m Metrics,
) (*chstorage.Inserter, *chstorage.Querier, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, nil, errors.Wrap(err, "parse DSN")
	}

	pass, _ := u.User.Password()
	opts := ch.Options{
		Logger:         lg.Named("ch"),
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
		return nil, nil, errors.Wrap(err, "migrate")
	}

	tables := chstorage.Tables{
		Spans: "traces_spans",
		Tags:  "traces_tags",
	}
	if err := tables.Create(ctx, c); err != nil {
		return nil, nil, errors.Wrap(err, "create tables")
	}

	inserter := chstorage.NewInserter(c, tables)
	querier := chstorage.NewQuerier(c, tables)
	return inserter, querier, nil
}
