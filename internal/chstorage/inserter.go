package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Inserter = (*Inserter)(nil)

// Inserter implements tracestorage.Inserter using Clickhouse.
type Inserter struct {
	ch     *chpool.Pool
	tables Tables

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
		opts.Tables = defaultTables
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
	return &Inserter{
		ch:     c,
		tables: opts.Tables,
		tracer: opts.TracerProvider.Tracer("Spans.Inserter"),
	}, nil
}

// InsertSpans inserts given spans.
func (i *Inserter) InsertSpans(ctx context.Context, spans []tracestorage.Span) (rerr error) {
	table := i.tables.Spans
	ctx, span := i.tracer.Start(ctx, "InsertSpans", trace.WithAttributes(
		attribute.Int("chstorage.spans_count", len(spans)),
		attribute.String("chstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	c := newSpanColumns()
	for _, s := range spans {
		c.AddRow(s)
	}
	input := c.Input()
	return i.ch.Do(ctx, ch.Query{
		Body:  input.Into(table),
		Input: input,
	})
}

// InsertTags insert given set of tags to the storage.
func (i *Inserter) InsertTags(ctx context.Context, tags map[tracestorage.Tag]struct{}) (rerr error) {
	table := i.tables.Tags
	ctx, span := i.tracer.Start(ctx, "InsertTags", trace.WithAttributes(
		attribute.Int("chstorage.tags_count", len(tags)),
		attribute.String("chstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		name      = new(proto.ColStr).LowCardinality()
		value     proto.ColStr
		valueType proto.ColEnum8
	)

	for tag := range tags {
		name.Append(tag.Name)
		value.Append(tag.Value)
		valueType.Append(proto.Enum8(tag.Type))
	}

	input := proto.Input{
		{Name: "name", Data: name},
		{Name: "value", Data: value},
		{Name: "value_type", Data: proto.Wrap(&valueType, valueTypeDDL)},
	}

	return i.ch.Do(ctx, ch.Query{
		Body:  input.Into(table),
		Input: input,
	})
}
