package oteldbexporter

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/exporter"

	"github.com/go-faster/oteldb/internal/chstorage"
)

// Config defines [Exporter] config.
type Config struct {
	DSL string `mapstructure:"dsl"`
}

func (c *Config) connect(ctx context.Context, settings exporter.CreateSettings) (*chstorage.Inserter, error) {
	pool, err := chstorage.Dial(ctx, c.DSL, chstorage.DialOptions{
		MeterProvider:  settings.MeterProvider,
		TracerProvider: settings.TracerProvider,
		Logger:         settings.Logger,
	})
	if err != nil {
		return nil, errors.Wrap(err, "dial clickhouse")
	}

	tables := chstorage.DefaultTables()
	if err := tables.Create(ctx, pool); err != nil {
		return nil, errors.Wrap(err, "create tables")
	}

	inserter, err := chstorage.NewInserter(pool, chstorage.InserterOptions{
		Tables:         tables,
		MeterProvider:  settings.MeterProvider,
		TracerProvider: settings.TracerProvider,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create inserter")
	}

	return inserter, nil
}
