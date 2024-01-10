package chstorage

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*Querier)(nil)

// Querier implements tracestorage.Querier using Clickhouse.
type Querier struct {
	ch     ClickhouseClient
	tables Tables
	tracer trace.Tracer

	clickhouseRequestHistogram metric.Float64Histogram
}

// QuerierOptions is Querier's options.
type QuerierOptions struct {
	// Tables provides table paths to query.
	Tables Tables
	// MeterProvider provides OpenTelemetry meter for this querier.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer for this querier.
	TracerProvider trace.TracerProvider
}

func (opts *QuerierOptions) setDefaults() {
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

// NewQuerier creates new Querier.
func NewQuerier(c ClickhouseClient, opts QuerierOptions) (*Querier, error) {
	// HACK(ernado): for some reason, we are getting no-op here.
	opts.TracerProvider = otel.GetTracerProvider()
	opts.MeterProvider = otel.GetMeterProvider()
	opts.setDefaults()

	meter := opts.MeterProvider.Meter("chstorage.Querier")
	clickhouseRequestHistogram, err := meter.Float64Histogram("chstorage.clickhouse.request",
		metric.WithUnit("s"),
		metric.WithDescription("Clickhouse request duration in seconds"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create clickhouse.request histogram metric")
	}
	return &Querier{
		ch:     c,
		tables: opts.Tables,
		tracer: opts.TracerProvider.Tracer("chstorage.Querier"),

		clickhouseRequestHistogram: clickhouseRequestHistogram,
	}, nil
}
