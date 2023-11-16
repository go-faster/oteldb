package chstorage

import (
	"github.com/ClickHouse/ch-go/chpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*Querier)(nil)

// Querier implements tracestorage.Querier using Clickhouse.
type Querier struct {
	ch     *chpool.Pool
	tables Tables
	tracer trace.Tracer
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
		opts.Tables = defaultTables
	}
	if opts.MeterProvider == nil {
		opts.MeterProvider = otel.GetMeterProvider()
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}
}

// NewQuerier creates new Querier.
func NewQuerier(c *chpool.Pool, opts QuerierOptions) (*Querier, error) {
	opts.setDefaults()

	return &Querier{
		ch:     c,
		tables: opts.Tables,
		tracer: otel.Tracer("chstorage.Querier"),
	}, nil
}
