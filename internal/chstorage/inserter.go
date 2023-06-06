package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Inserter = (*Inserter)(nil)

// Inserter implements tracestorage.Inserter using Clickhouse.
type Inserter struct {
	ch     *chpool.Pool
	tables Tables
}

// NewInserter creates new Inserter.
func NewInserter(c *chpool.Pool, tables Tables) *Inserter {
	return &Inserter{
		ch:     c,
		tables: tables,
	}
}

// InsertSpans inserts given spans.
func (i *Inserter) InsertSpans(ctx context.Context, spans []tracestorage.Span) error {
	c := newSpanColumns()
	for _, s := range spans {
		c.Append(s)
	}
	input := c.Input()
	return i.ch.Do(ctx, ch.Query{
		Body:  input.Into(i.tables.Spans),
		Input: input,
	})
}

// InsertTags insert given set of tags to the storage.
func (i *Inserter) InsertTags(ctx context.Context, tags map[tracestorage.Tag]struct{}) error {
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
		Body:  input.Into(i.tables.Tags),
		Input: input,
	})
}
