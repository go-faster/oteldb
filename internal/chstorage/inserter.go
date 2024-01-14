package chstorage

import (
	"context"
	"runtime"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Inserter = (*Inserter)(nil)

// Inserter implements tracestorage.Inserter using Clickhouse.
type Inserter struct {
	ch     ClickhouseClient
	tables Tables

	metricBatches chan pmetric.Metrics

	insertedPoints  metric.Int64Counter
	insertedSpans   metric.Int64Counter
	insertedTags    metric.Int64Counter
	insertedRecords metric.Int64Counter
	inserts         metric.Int64Counter

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

	// Sync disables batching.
	Sync bool
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
func NewInserter(c ClickhouseClient, opts InserterOptions) (*Inserter, error) {
	// HACK(ernado): for some reason, we are getting no-op here.
	opts.TracerProvider = otel.GetTracerProvider()
	opts.MeterProvider = otel.GetMeterProvider()
	opts.setDefaults()

	meter := opts.MeterProvider.Meter("chstorage.Inserter")
	insertedSpans, err := meter.Int64Counter("chstorage.traces.inserted_spans",
		metric.WithDescription("Number of inserted spans"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create inserted_spans")
	}
	insertedTags, err := meter.Int64Counter("chstorage.traces.inserted_tags",
		metric.WithDescription("Number of inserted tags"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create inserted_tags")
	}
	insertedRecords, err := meter.Int64Counter("chstorage.logs.inserted_records",
		metric.WithDescription("Number of inserted log records"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create inserted_records")
	}
	insertedPoints, err := meter.Int64Counter("chstorage.metrics.inserted_points",
		metric.WithDescription("Number of inserted points"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create inserted_points")
	}
	inserts, err := meter.Int64Counter("chstorage.inserts",
		metric.WithDescription("Number of insert invocations"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create inserts")
	}

	inserter := &Inserter{
		ch:              c,
		tables:          opts.Tables,
		insertedSpans:   insertedSpans,
		insertedTags:    insertedTags,
		insertedRecords: insertedRecords,
		insertedPoints:  insertedPoints,
		inserts:         inserts,
		tracer:          opts.TracerProvider.Tracer("chstorage.Inserter"),
	}

	if !opts.Sync {
		jobs := runtime.GOMAXPROCS(0)
		inserter.metricBatches = make(chan pmetric.Metrics, jobs)
		ctx := context.Background()
		for i := 0; i < jobs; i++ {
			go func() {
				if err := inserter.saveMetricBatches(ctx); err != nil {
					panic(err)
				}
			}()
		}
	}
	return inserter, nil
}
