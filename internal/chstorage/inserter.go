package chstorage

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/autometric"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Inserter = (*Inserter)(nil)

// Inserter implements tracestorage.Inserter using Clickhouse.
type Inserter struct {
	ch     ClickhouseClient
	tables Tables

	stats struct {
		// Logs.
		InsertedRecords metric.Int64Counter `name:"logs.inserted_records" description:"Number of inserted log records"`
		// Metrics.
		InsertedPoints       metric.Int64Counter `name:"metrics.inserted_points" description:"Number of inserted points"`
		InsertedHistograms   metric.Int64Counter `name:"metrics.inserted_histograms" description:"Number of inserted exponential (native) histograms"`
		InsertedExemplars    metric.Int64Counter `name:"metrics.inserted_exemplars" description:"Number of inserted exemplars"`
		InsertedMetricLabels metric.Int64Counter `name:"metrics.inserted_metric_labels" description:"Number of inserted metric labels"`
		// Traces.
		InsertedSpans metric.Int64Counter `name:"traces.inserted_spans" description:"Number of inserted spans"`
		InsertedTags  metric.Int64Counter `name:"traces.inserted_tags" description:"Number of inserted trace attributes"`
		// Common.
		Inserts metric.Int64Counter `name:"inserts" description:"Number of insert invocations"`
	}
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
func NewInserter(c ClickhouseClient, opts InserterOptions) (*Inserter, error) {
	// HACK(ernado): for some reason, we are getting no-op here.
	opts.TracerProvider = otel.GetTracerProvider()
	opts.MeterProvider = otel.GetMeterProvider()
	opts.setDefaults()

	inserter := &Inserter{
		ch:     c,
		tables: opts.Tables,
		tracer: opts.TracerProvider.Tracer("chstorage.Inserter"),
	}

	meter := opts.MeterProvider.Meter("chstorage.Inserter")
	if err := autometric.Init(meter, &inserter.stats, autometric.InitOptions{
		Prefix: "chstorage.",
	}); err != nil {
		return nil, errors.Wrap(err, "init stats")
	}

	return inserter, nil
}
