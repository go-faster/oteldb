package chstorage

import (
	"github.com/ClickHouse/ch-go/chpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Inserter = (*Inserter)(nil)

// Inserter implements tracestorage.Inserter using Clickhouse.
type Inserter struct {
	ch     *chpool.Pool
	tables Tables

	insertedSpans metric.Int64Counter
	insertedTags  metric.Int64Counter

	tracer trace.Tracer
}

// InserterOptions is Inserter's options.
type InserterOptions struct {
	// Tables provides table paths to query.
	Tables Tables
	// MeterProvider provides OpenTelemetry meter for this querier.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer for this querier.
	TracerProvider trace.TracerProvider
}

func (opts *InserterOptions) setDefaults() {
	if opts.Tables == (Tables{}) {
		opts.Tables = DefaultTables()
	}
	if opts.MeterProvider == nil {
		opts.MeterProvider = otel.GetMeterProvider()
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}
}

// NewInserter creates new Inserter.
func NewInserter(c *chpool.Pool, opts InserterOptions) (*Inserter, error) {
	opts.setDefaults()

	meter := opts.MeterProvider.Meter("chstorage.Inserter")
	insertedSpans, err := meter.Int64Counter("chstorage.traces.inserted_spans")
	if err != nil {
		return nil, errors.Wrap(err, "create inserted_spans")
	}
	insertedTags, err := meter.Int64Counter("chstorage.traces.inserted_tags")
	if err != nil {
		return nil, errors.Wrap(err, "create inserted_tags")
	}

	return &Inserter{
		ch:            c,
		tables:        opts.Tables,
		insertedSpans: insertedSpans,
		insertedTags:  insertedTags,
		tracer:        opts.TracerProvider.Tracer("chstorage.Inserter"),
	}, nil
}
