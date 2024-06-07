package chstorage

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*Querier)(nil)

// Querier implements tracestorage.Querier using Clickhouse.
type Querier struct {
	ch     ClickhouseClient
	tables Tables

	clickhouseRequestHistogram metric.Float64Histogram
	tracer                     trace.Tracer
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

type selectQuery struct {
	Query    *chsql.SelectQuery
	OnResult chsql.OnResult

	ExternalData  []proto.InputColumn
	ExternalTable string

	Type   string
	Signal string
	Table  string
}

func (q *Querier) do(ctx context.Context, s selectQuery) error {
	query, err := s.Query.Prepare(s.OnResult)
	if err != nil {
		return errors.Wrap(err, "build query")
	}
	query.ExternalData = s.ExternalData
	query.ExternalTable = s.ExternalTable
	query.Logger = zctx.From(ctx).Named("ch")

	queryStartTime := time.Now()
	if err := q.ch.Do(ctx, query); err != nil {
		return err
	}

	q.clickhouseRequestHistogram.Record(ctx, time.Since(queryStartTime).Seconds(),
		metric.WithAttributes(
			attribute.String("chstorage.query_type", s.Type),
			attribute.String("chstorage.table", s.Table),
			attribute.String("chstorage.signal", s.Signal),
		),
	)
	return nil
}
