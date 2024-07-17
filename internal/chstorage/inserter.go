package chstorage

import (
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

	// Logs metrics.
	insertedRecords metric.Int64Counter
	// Metrics metrics (it counts metrics insertion).
	insertedPoints       metric.Int64Counter
	insertedHistograms   metric.Int64Counter
	insertedExemplars    metric.Int64Counter
	insertedMetricLabels metric.Int64Counter
	// Trace metrics.
	insertedSpans metric.Int64Counter
	insertedTags  metric.Int64Counter
	// Common metrics.
	inserts metric.Int64Counter

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
	for _, desc := range []struct {
		ptr         *metric.Int64Counter
		name        string
		description string
	}{
		// Logs.
		{&inserter.insertedRecords, "chstorage.logs.inserted_records", "Number of inserted log records"},
		// Metrics.
		{&inserter.insertedPoints, "chstorage.metrics.inserted_points", "Number of inserted points"},
		{&inserter.insertedHistograms, "chstorage.metrics.inserted_histograms", "Number of inserted exponential (native) histograms"},
		{&inserter.insertedExemplars, "chstorage.metrics.inserted_exemplars", "Number of inserted exemplars"},
		{&inserter.insertedMetricLabels, "chstorage.metrics.inserted_metric_labels", "Number of inserted metric labels"},
		// Traces.
		{&inserter.insertedSpans, "chstorage.traces.inserted_spans", "Number of inserted spans"},
		{&inserter.insertedTags, "chstorage.traces.inserted_tags", "Number of inserted trace attributes"},
		// Common.
		{&inserter.inserts, "chstorage.inserts", "Number of insert invocations"},
	} {
		counter, err := meter.Int64Counter(desc.name,
			metric.WithDescription(desc.description),
			metric.WithUnit("1"),
		)
		if err != nil {
			return nil, errors.Wrapf(err, "create %q", desc.name)
		}
		*desc.ptr = counter
	}

	return inserter, nil
}
