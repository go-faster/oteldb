package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

// InsertSpans inserts given spans.
func (i *Inserter) InsertSpans(ctx context.Context, spans []tracestorage.Span) (rerr error) {
	table := i.tables.Spans
	ctx, span := i.tracer.Start(ctx, "chstorage.traces.InsertSpans", trace.WithAttributes(
		attribute.Int("chstorage.spans_count", len(spans)),
		attribute.String("chstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			i.stats.InsertedSpans.Add(ctx, int64(len(spans)))
			i.stats.Inserts.Add(ctx, 1,
				metric.WithAttributes(
					attribute.String("chstorage.table", table),
					attribute.String("chstorage.signal", "traces"),
				),
			)
		}
		span.End()
	}()

	c := newSpanColumns()
	for _, s := range spans {
		c.AddRow(s)
	}
	input := c.Input()
	return i.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   input.Into(table),
		Input:  input,
	})
}

// InsertTags insert given set of tags to the storage.
func (i *Inserter) InsertTags(ctx context.Context, tags map[tracestorage.Tag]struct{}) (rerr error) {
	table := i.tables.Tags
	ctx, span := i.tracer.Start(ctx, "chstorage.traces.InsertTags", trace.WithAttributes(
		attribute.Int("chstorage.tags_count", len(tags)),
		attribute.String("chstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			i.stats.InsertedTags.Add(ctx, int64(len(tags)))
			i.stats.Inserts.Add(ctx, 1,
				metric.WithAttributes(
					attribute.String("chstorage.table", table),
					attribute.String("chstorage.signal", "tags"),
				),
			)
		}
		span.End()
	}()

	var (
		name      = new(proto.ColStr).LowCardinality()
		value     proto.ColStr
		valueType proto.ColEnum8
		scopeType proto.ColEnum8
	)

	for tag := range tags {
		name.Append(tag.Name)
		value.Append(tag.Value)
		valueType.Append(proto.Enum8(tag.Type))
		scopeType.Append(proto.Enum8(tag.Scope))
	}

	input := proto.Input{
		{Name: "name", Data: name},
		{Name: "value", Data: value},
		{Name: "value_type", Data: proto.Wrap(&valueType, valueTypeDDL)},
		{Name: "scope", Data: proto.Wrap(&scopeType, scopeTypeDDL)},
	}

	return i.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   input.Into(table),
		Input:  input,
	})
}
