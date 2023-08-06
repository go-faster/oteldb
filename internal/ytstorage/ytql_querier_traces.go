package ytstorage

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/exp/maps"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*YTQLQuerier)(nil)

// SearchTags performs search by given tags.
func (q *YTQLQuerier) SearchTags(ctx context.Context, tags map[string]string, opts tracestorage.SearchTagsOptions) (iterators.Iterator[tracestorage.Span], error) {
	var traceIDQuery strings.Builder

	fmt.Fprintf(&traceIDQuery, "trace_id FROM [%s] WHERE true", q.tables.spans)
	if s := opts.Start; s != 0 {
		fmt.Fprintf(&traceIDQuery, " AND start >= %d", s)
	}
	if e := opts.End; e != 0 {
		fmt.Fprintf(&traceIDQuery, " AND end <= %d", e)
	}
	if d := opts.MinDuration; d != 0 {
		fmt.Fprintf(&traceIDQuery, " AND (end-start) >= %d", d)
	}
	if d := opts.MaxDuration; d != 0 {
		fmt.Fprintf(&traceIDQuery, " AND (end-start) <= %d", d)
	}
	for key, value := range tags {
		if key == "name" {
			fmt.Fprintf(&traceIDQuery, " AND name = %q", value)
			continue
		}

		traceIDQuery.WriteString(" AND (")
		for i, column := range []string{
			"attrs",
			"scope_attrs",
			"resource_attrs",
		} {
			if i != 0 {
				traceIDQuery.WriteString(" OR ")
			}
			yp := append([]byte{'/'}, key...)
			yp = append(yp, "/1"...)
			fmt.Fprintf(&traceIDQuery, "try_get_string(%s, %q) = %q", column, yp, value)
		}
		traceIDQuery.WriteByte(')')
	}
	return q.querySets(ctx, traceIDQuery.String())
}

// TagNames returns all available tag names.
func (q *YTQLQuerier) TagNames(ctx context.Context) ([]string, error) {
	query := fmt.Sprintf("name FROM [%s]", q.tables.tags)
	names := map[string]struct{}{}
	err := queryRows(ctx, q.yc, query, func(tag tracestorage.Tag) {
		names[tag.Name] = struct{}{}
	})
	return maps.Keys(names), err
}

// TagValues returns all available tag values for given tag.
func (q *YTQLQuerier) TagValues(ctx context.Context, tagName string) (iterators.Iterator[tracestorage.Tag], error) {
	query := fmt.Sprintf("* FROM [%s] WHERE name = %q", q.tables.tags, tagName)
	r, err := q.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[tracestorage.Tag]{reader: r}, nil
}

// TraceByID returns spans of given trace.
func (q *YTQLQuerier) TraceByID(ctx context.Context, id otelstorage.TraceID, opts tracestorage.TraceByIDOptions) (iterators.Iterator[tracestorage.Span], error) {
	query := fmt.Sprintf("* FROM [%s] WHERE trace_id = %q", q.tables.spans, id[:])

	if s := opts.Start; s != 0 {
		query += fmt.Sprintf(" AND start >= %d", s)
	}
	if e := opts.End; e != 0 {
		query += fmt.Sprintf(" AND end <= %d", e)
	}

	r, err := q.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[tracestorage.Span]{reader: r}, nil
}

var _ traceqlengine.Querier = (*YTQLQuerier)(nil)

// SelectSpansets get spansets from storage.
func (q *YTQLQuerier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (iterators.Iterator[traceqlengine.Trace], error) {
	var traceIDQuery strings.Builder

	fmt.Fprintf(&traceIDQuery, "trace_id FROM [%s] WHERE true", q.tables.spans)
	if s := params.Start; s != 0 {
		fmt.Fprintf(&traceIDQuery, " AND start >= %d", s)
	}
	if e := params.End; e != 0 {
		fmt.Fprintf(&traceIDQuery, " AND end <= %d", e)
	}

	for _, matcher := range params.Matchers {
		if matcher.Op == 0 {
			if params.Op == traceql.SpansetOpAnd {
				traceIDQuery.WriteString(" AND ")
			} else {
				traceIDQuery.WriteString(" OR ")
			}

			// Just query spans with this attribute.
			attr := matcher.Attribute
			traceIDQuery.WriteByte('(')
			pathBuf := make([]byte, 0, 32)
			for i, column := range getAttributeColumns(attr) {
				if i != 0 {
					traceIDQuery.WriteString(" OR ")
				}
				pathBuf = append(pathBuf[:0], '/')
				pathBuf = append(pathBuf, attr.Name...)
				fmt.Fprintf(&traceIDQuery, `NOT is_null(try_get_any(%s, %q))`, column, pathBuf)
			}
			traceIDQuery.WriteByte(')')
		}

		var cmp string
		switch matcher.Op {
		case traceql.OpEq:
			cmp = "="
		case traceql.OpNotEq:
			cmp = "!="
		case traceql.OpGt:
			cmp = ">"
		case traceql.OpGte:
			cmp = ">="
		case traceql.OpLt:
			cmp = "<"
		case traceql.OpLte:
			cmp = "<="
		default:
			// Unsupported for now.
			continue
		}

		var value, getter string
		switch s := matcher.Static; s.Type {
		case traceql.TypeString:
			value = strconv.Quote(s.Str)
			getter = "try_get_string"
		case traceql.TypeInt:
			value = strconv.FormatInt(s.AsInt(), 10)
			getter = "try_get_int64"
		case traceql.TypeNumber:
			value = strconv.FormatFloat(s.AsNumber(), 'f', -1, 64)
			getter = "try_get_double"
		case traceql.TypeBool:
			if s.AsBool() {
				value = "true"
			} else {
				value = "false"
			}
			getter = "try_get_boolean"
		case traceql.TypeDuration:
			value = strconv.FormatInt(s.AsDuration().Nanoseconds(), 10)
			getter = "try_get_uint64"
		case traceql.TypeSpanStatus:
			value = strconv.Itoa(int(s.AsSpanStatus()))
			getter = "try_get_int64"
		case traceql.TypeSpanKind:
			value = strconv.Itoa(int(s.AsSpanKind()))
			getter = "try_get_int64"
		default:
			// Unsupported for now.
			continue
		}

		if params.Op == traceql.SpansetOpAnd {
			traceIDQuery.WriteString(" AND ")
		} else {
			traceIDQuery.WriteString(" OR ")
		}

		switch attr := matcher.Attribute; attr.Prop {
		case traceql.SpanDuration:
			fmt.Fprintf(&traceIDQuery, "(end-start) %s %s", cmp, value)
		case traceql.SpanName:
			fmt.Fprintf(&traceIDQuery, "name %s %s", cmp, value)
		case traceql.SpanStatus:
			fmt.Fprintf(&traceIDQuery, "status_code %s %s", cmp, value)
		case traceql.SpanKind:
			fmt.Fprintf(&traceIDQuery, "kind %s %s", cmp, value)
		case traceql.SpanParent,
			traceql.SpanChildCount,
			traceql.RootSpanName,
			traceql.RootServiceName,
			traceql.TraceDuration:
			// Unsupported yet.
			traceIDQuery.WriteString("true")
		default:
			// SpanAttribute
			traceIDQuery.WriteByte('(')
			pathBuf := make([]byte, 0, 32)
			for i, column := range getAttributeColumns(attr) {
				if i != 0 {
					traceIDQuery.WriteString(" OR ")
				}
				pathBuf = append(pathBuf[:0], '/')
				pathBuf = append(pathBuf, attr.Name...)
				pathBuf = append(pathBuf, "/2"...)
				fmt.Fprintf(&traceIDQuery, "%s(%s, %q) %s %s", getter, column, pathBuf, cmp, value)
			}
			traceIDQuery.WriteByte(')')
		}
	}

	iter, err := q.querySets(ctx, traceIDQuery.String())
	if err != nil {
		return nil, errors.Wrap(err, "query sets")
	}
	defer func() {
		_ = iter.Close()
	}()

	var (
		traces = map[otelstorage.TraceID][]tracestorage.Span{}
		span   tracestorage.Span
	)
	for iter.Next(&span) {
		traces[span.TraceID] = append(traces[span.TraceID], span)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	result := make([]traceqlengine.Trace, 0, len(traces))
	for id, spans := range traces {
		result = append(result, traceqlengine.Trace{
			TraceID: id,
			Spans:   spans,
		})
	}
	return iterators.Slice(result), nil
}

func getAttributeColumns(attr traceql.Attribute) []string {
	if attr.Prop != traceql.SpanAttribute || attr.Parent {
		return nil
	}
	switch attr.Scope {
	case traceql.ScopeNone:
		return []string{
			"attrs",
			"scope_attrs",
			"resource_attrs",
		}
	case traceql.ScopeResource:
		return []string{
			"scope_attrs",
			"resource_attrs",
		}
	case traceql.ScopeSpan:
		return []string{
			"attrs",
		}
	default:
		return nil
	}
}

func (q *YTQLQuerier) querySets(ctx context.Context, traceIDQuery string) (iterators.Iterator[tracestorage.Span], error) {
	// Query trace IDs first.
	traces := map[otelstorage.TraceID]struct{}{}
	if err := queryRows(ctx, q.yc, traceIDQuery, func(s tracestorage.Span) {
		traces[s.TraceID] = struct{}{}
	}); err != nil {
		return nil, errors.Wrap(err, "query traceIDs")
	}

	if len(traces) == 0 {
		return iterators.Empty[tracestorage.Span](), nil
	}

	// Then, query all spans for each found trace ID.
	var query strings.Builder
	fmt.Fprintf(&query, "* FROM [%s] WHERE trace_id IN (", q.tables.spans)
	n := 0
	for id := range traces {
		if n != 0 {
			query.WriteByte(',')
		}
		fmt.Fprintf(&query, "%q", id)
		n++
	}
	query.WriteByte(')')

	r, err := q.yc.SelectRows(ctx, query.String(), nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[tracestorage.Span]{reader: r}, nil
}
