package main

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/prometheus/prometheus/storage"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type otelStorage struct {
	logQuerier     logQuerier
	traceQuerier   traceQuerier
	metricsQuerier metricQuerier
}

type logQuerier interface {
	logstorage.Querier
	logqlengine.Querier
}

type traceQuerier interface {
	tracestorage.Querier
	traceqlengine.Querier
}

type metricQuerier interface {
	storage.Queryable
	storage.ExemplarQueryable
}

func setupCH(
	ctx context.Context,
	dsn string,
	ttl time.Duration,
	lg *zap.Logger,
	m *app.Metrics,
) (store otelStorage, _ error) {
	c, err := chstorage.Dial(ctx, dsn, chstorage.DialOptions{
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),
		Logger:         lg,
	})
	if err != nil {
		return store, errors.Wrap(err, "dial clickhouse")
	}

	// FIXME(tdakkota): this is not a good place for migration
	tables := chstorage.DefaultTables()
	tables.TTL = ttl

	if err := tables.Create(ctx, c); err != nil {
		return store, errors.Wrap(err, "create tables")
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
		logQuerier:     querier,
		traceQuerier:   querier,
		metricsQuerier: querier,
	}, nil
}
