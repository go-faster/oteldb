package chstorage

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*Querier)(nil)

// Querier implements tracestorage.Querier using Clickhouse.
type Querier struct {
	ch     *chpool.Pool
	tables Tables
}

// NewQuerier creates new Querier.
func NewQuerier(c *chpool.Pool, tables Tables) *Querier {
	return &Querier{
		ch:     c,
		tables: tables,
	}
}

// SearchTags performs search by given tags.
func (q *Querier) SearchTags(ctx context.Context, tags map[string]string, opts tracestorage.SearchTagsOptions) (tracestorage.Iterator[tracestorage.Span], error) {
	var query strings.Builder
	fmt.Fprintf(&query, `SELECT * FROM %#[1]q WHERE trace_id IN (
		SELECT trace_id FROM %#[1]q WHERE true
	`, q.tables.Spans)
	for key, value := range tags {
		if key == "name" {
			fmt.Fprintf(&query, " AND name = %s", singleQuoted(value))
			continue
		}

		query.WriteString(" AND (")
		for i, prefix := range []string{
			"attrs",
			"scope_attrs",
			"resource_attrs",
		} {
			if i != 0 {
				query.WriteString(" OR ")
			}
			for i, column := range []string{
				"str",
				"int",
				"float",
				"bool",
				"bytes",
			} {
				if i != 0 {
					query.WriteString(" OR ")
				}
				fmt.Fprintf(&query,
					`toString(arrayElement(%[1]s_%[2]s_keys, indexOf(%[1]s_%[2]s_keys, %[3]s))) = %[4]s`,
					prefix, column, singleQuoted(key), singleQuoted(value),
				)
			}
			query.WriteByte('\n')
		}
		query.WriteByte(')')
	}
	query.WriteByte(')')

	if s := opts.Start; s != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(start) >= %d", s)
	}
	if e := opts.End; e != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(end) <= %d", e)
	}
	if d := opts.MinDuration; d != 0 {
		fmt.Fprintf(&query, " AND (toUnixTimestamp64Nano(end) - toUnixTimestamp64Nano(start)) >= %d", d)
	}
	if d := opts.MaxDuration; d != 0 {
		fmt.Fprintf(&query, " AND (toUnixTimestamp64Nano(end) - toUnixTimestamp64Nano(start)) <= %d", d)
	}
	return q.querySpans(ctx, query.String())
}

// TagNames returns all available tag names.
func (q *Querier) TagNames(ctx context.Context) (r []string, _ error) {
	data := new(proto.ColStr).LowCardinality()
	if err := q.ch.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT DISTINCT name FROM %#q", q.tables.Tags),
		Result: proto.ResultColumn{
			Name: "name",
			Data: data,
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			r = append(r, data.Values...)
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}
	return r, nil
}

// TagValues returns all available tag values for given tag.
func (q *Querier) TagValues(ctx context.Context, tagName string) (tracestorage.Iterator[tracestorage.Tag], error) {
	var (
		value     proto.ColStr
		valueType proto.ColEnum8

		r []tracestorage.Tag
	)

	if err := q.ch.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT DISTINCT value, value_type FROM %#q WHERE name = %s", q.tables.Tags, singleQuoted(tagName)),
		Result: proto.Results{
			{Name: "value", Data: &value},
			{Name: "value_type", Data: proto.Wrap(&valueType, valueTypeDDL)},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			return value.ForEach(func(i int, value string) error {
				typ := valueType.Row(i)
				r = append(r, tracestorage.Tag{
					Name:  tagName,
					Value: value,
					Type:  int32(typ),
				})
				return nil
			})
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}

	return tracestorage.NewSliceIterator(r), nil
}

// TraceByID returns spans of given trace.
func (q *Querier) TraceByID(ctx context.Context, id tracestorage.TraceID, opts tracestorage.TraceByIDOptions) (tracestorage.Iterator[tracestorage.Span], error) {
	query := fmt.Sprintf("SELECT * FROM %#q WHERE trace_id = %s", q.tables.Spans, singleQuoted(id.Hex()))
	if s := opts.Start; s != 0 {
		query += fmt.Sprintf(" AND toUnixTimestamp64Nano(start) >= %d", s)
	}
	if e := opts.End; e != 0 {
		query += fmt.Sprintf(" AND toUnixTimestamp64Nano(end) <= %d", e)
	}
	return q.querySpans(ctx, query)
}

func (q *Querier) querySpans(ctx context.Context, query string) (tracestorage.Iterator[tracestorage.Span], error) {
	c := newSpanColumns()

	var r []tracestorage.Span
	if err := q.ch.Do(ctx, ch.Query{
		Body:   query,
		Result: c.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			r = c.CollectAppend(r)
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}

	return tracestorage.NewSliceIterator(r), nil
}
