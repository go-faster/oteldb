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
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*Querier)(nil)

// Querier implements tracestorage.Querier using Clickhouse.
type Querier struct {
	ch         ClickhouseClient
	tables     Tables
	labelLimit int

	clickhouseRequestHistogram metric.Float64Histogram
	tracer                     trace.Tracer
}

// QuerierOptions is Querier's options.
type QuerierOptions struct {
	// Tables provides table paths to query.
	Tables Tables
	// LabelLimit defines limit for label lookup in the main table.
	LabelLimit int
	// MeterProvider provides OpenTelemetry meter for this querier.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer for this querier.
	TracerProvider trace.TracerProvider
}

func (opts *QuerierOptions) setDefaults() {
	if opts.Tables == (Tables{}) {
		opts.Tables = DefaultTables()
	}
	if opts.LabelLimit == 0 {
		opts.LabelLimit = 1000
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
	if _, ok := opts.TracerProvider.(noop.TracerProvider); ok {
		opts.TracerProvider = otel.GetTracerProvider()
	}
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
		ch:         c,
		tables:     opts.Tables,
		labelLimit: opts.LabelLimit,

		tracer:                     opts.TracerProvider.Tracer("chstorage.Querier"),
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
	lg := zctx.From(ctx)

	query, err := s.Query.Prepare(s.OnResult)
	if err != nil {
		return errors.Wrap(err, "build query")
	}
	query.ExternalData = s.ExternalData
	query.ExternalTable = s.ExternalTable
	query.Logger = lg.Named("ch")

	queryStartTime := time.Now()
	err = q.ch.Do(ctx, query)
	took := time.Since(queryStartTime)
	if ce := lg.Check(zap.DebugLevel, "Query Clickhouse"); ce != nil {
		errField := zap.Skip()
		if err != nil {
			errField = zap.Error(err)
		}
		ce.Write(
			zap.String("query_type", s.Type),
			zap.String("table", s.Table),
			zap.String("signal", s.Signal),
			zap.Duration("took", took),
			errField,
		)
	}
	if err != nil {
		return errors.Wrapf(err, "execute %s (signal: %s)", s.Type, s.Signal)
	}

	q.clickhouseRequestHistogram.Record(ctx, took.Seconds(),
		metric.WithAttributes(
			attribute.String("chstorage.query_type", s.Type),
			attribute.String("chstorage.table", s.Table),
			attribute.String("chstorage.signal", s.Signal),
		),
	)
	return nil
}
