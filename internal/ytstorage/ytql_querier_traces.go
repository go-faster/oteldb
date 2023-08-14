package ytstorage

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*YTQLQuerier)(nil)

// SearchTags performs search by given tags.
func (q *YTQLQuerier) SearchTags(ctx context.Context, tags map[string]string, opts tracestorage.SearchTagsOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.spans

	ctx, span := q.tracer.Start(ctx, "SearchTags",
		trace.WithAttributes(
			attribute.Int("ytstorage.tags_count", len(tags)),
			attribute.Int64("ytstorage.start_range", int64(opts.Start)),
			attribute.Int64("ytstorage.end_range", int64(opts.End)),
			attribute.Int64("ytstorage.max_duration", int64(opts.MaxDuration)),
			attribute.Int64("ytstorage.min_duration", int64(opts.MinDuration)),
			attribute.Stringer("ytstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

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
	return q.queryTraces(ctx, traceIDQuery.String())
}

// TagNames returns all available tag names.
func (q *YTQLQuerier) TagNames(ctx context.Context) (_ []string, rerr error) {
	table := q.tables.tags

	ctx, span := q.tracer.Start(ctx, "TagNames",
		trace.WithAttributes(
			attribute.Stringer("ytstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	query := fmt.Sprintf("name FROM [%s]", table)
	names := map[string]struct{}{}
	err := queryRows(ctx, q.yc, query, func(tag tracestorage.Tag) {
		names[tag.Name] = struct{}{}
	})
	return maps.Keys(names), err
}

// TagValues returns all available tag values for given tag.
func (q *YTQLQuerier) TagValues(ctx context.Context, tagName string) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	table := q.tables.tags

	ctx, span := q.tracer.Start(ctx, "TagValues",
		trace.WithAttributes(
			attribute.String("ytstorage.tag_to_query", tagName),
			attribute.Stringer("ytstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	query := fmt.Sprintf("* FROM [%s] WHERE name = %q", table, tagName)
	r, err := q.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[tracestorage.Tag]{reader: r}, nil
}

// TraceByID returns spans of given trace.
func (q *YTQLQuerier) TraceByID(ctx context.Context, id otelstorage.TraceID, opts tracestorage.TraceByIDOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.spans

	ctx, span := q.tracer.Start(ctx, "TraceByID",
		trace.WithAttributes(
			attribute.String("ytstorage.id_to_query", id.Hex()),
			attribute.Int64("ytstorage.start_range", int64(opts.Start)),
			attribute.Int64("ytstorage.end_range", int64(opts.End)),
			attribute.Stringer("ytstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	query := fmt.Sprintf("* FROM [%s] WHERE trace_id = %q", table, id[:])

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
func (q *YTQLQuerier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (_ iterators.Iterator[traceqlengine.Trace], rerr error) {
	ctx, span := q.tracer.Start(ctx, "SelectSpansets",
		trace.WithAttributes(
			attribute.String("ytstorage.span_matcher_operation", params.Op.String()),
			attribute.Int("ytstorage.span_matchers", len(params.Matchers)),
			attribute.Int64("ytstorage.start_range", int64(params.Start)),
			attribute.Int64("ytstorage.end_range", int64(params.End)),
			attribute.Int64("ytstorage.max_duration", int64(params.MaxDuration)),
			attribute.Int64("ytstorage.min_duration", int64(params.MinDuration)),
			attribute.Int("ytstorage.limit", params.Limit),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	query := q.buildSpansetsQuery(span, params)

	iter, err := q.queryTraces(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "query traces")
	}
	defer func() {
		_ = iter.Close()
	}()

	var (
		traces = map[otelstorage.TraceID][]tracestorage.Span{}
		val    tracestorage.Span
	)
	for iter.Next(&val) {
		traces[val.TraceID] = append(traces[val.TraceID], val)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	var (
		result     = make([]traceqlengine.Trace, 0, len(traces))
		spansCount int
	)
	for id, spans := range traces {
		spansCount += len(spans)
		result = append(result, traceqlengine.Trace{
			TraceID: id,
			Spans:   spans,
		})
	}
	span.SetAttributes(
		attribute.Int("ytstorage.queried_spans", spansCount),
		attribute.Int("ytstorage.queried_traces", len(result)),
	)

	return iterators.Slice(result), nil
}

func (q *YTQLQuerier) buildSpansetsQuery(span trace.Span, params traceqlengine.SelectSpansetsParams) string {
	var (
		traceIDQuery strings.Builder
		table        = q.tables.spans
	)

	fmt.Fprintf(&traceIDQuery, "trace_id FROM [%s] WHERE true", table)
	if s := params.Start; s != 0 {
		fmt.Fprintf(&traceIDQuery, "\nAND start >= %d", s)
	}
	if e := params.End; e != 0 {
		fmt.Fprintf(&traceIDQuery, "\nAND end <= %d", e)
	}

	var (
		dropped   int
		pathBuf   = make([]byte, 0, 32)
		writeNext = func() {
			if params.Op == traceql.SpansetOpAnd {
				traceIDQuery.WriteString("\nAND ")
			} else {
				traceIDQuery.WriteString("\nOR ")
			}
		}
	)
	traceIDQuery.WriteString("\nAND ( true")
	for _, matcher := range params.Matchers {
		if matcher.Op == 0 {
			writeNext()

			// Just query spans with this attribute.
			attr := matcher.Attribute
			traceIDQuery.WriteString("(\n")
			for i, column := range getAttributeColumns(attr) {
				if i != 0 {
					traceIDQuery.WriteString("\nOR ")
				}
				pathBuf = append(pathBuf[:0], '/')
				pathBuf = append(pathBuf, attr.Name...)
				fmt.Fprintf(&traceIDQuery, `NOT is_null(try_get_any(%s, %q))`, column, pathBuf)
			}
			traceIDQuery.WriteString("\n)")
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
			dropped++
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
			dropped++
			continue
		}

		writeNext()
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
			dropped++
			traceIDQuery.WriteString("true")
		default:
			// SpanAttribute
			traceIDQuery.WriteString("(\n")
			for i, column := range getAttributeColumns(attr) {
				if i != 0 {
					traceIDQuery.WriteString("\nOR ")
				}
				pathBuf = append(pathBuf[:0], '/')
				pathBuf = append(pathBuf, attr.Name...)
				pathBuf = append(pathBuf, "/2"...)
				fmt.Fprintf(&traceIDQuery, "%s(%s, %q) %s %s", getter, column, pathBuf, cmp, value)
			}
			traceIDQuery.WriteString("\n)")
		}
	}
	traceIDQuery.WriteString("\n)")

	span.SetAttributes(
		attribute.Int("ytstorage.unsupported_span_matchers", dropped),
		attribute.Stringer("ytstorage.table", table),
	)

	return traceIDQuery.String()
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

func (q *YTQLQuerier) queryTraces(ctx context.Context, traceIDQuery string) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	// Query trace IDs first.
	traces, err := q.queryTraceIDs(ctx, traceIDQuery)
	if err != nil {
		return nil, errors.Wrap(err, "query traceIDs")
	}

	if len(traces) == 0 {
		return iterators.Empty[tracestorage.Span](), nil
	}

	// Then query spans.
	iter, err := q.querySpans(ctx, traces)
	if err != nil {
		return nil, errors.Wrap(err, "query spans")
	}

	return iter, nil
}

func (q *YTQLQuerier) queryTraceIDs(ctx context.Context, query string) (_ map[otelstorage.TraceID]struct{}, rerr error) {
	ctx, span := q.tracer.Start(ctx, "QueryTraceIDs")
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	traces := map[otelstorage.TraceID]struct{}{}
	if err := queryRows(ctx, q.yc, query, func(s tracestorage.Span) {
		traces[s.TraceID] = struct{}{}
	}); err != nil {
		return nil, err
	}

	return traces, nil
}

func (q *YTQLQuerier) querySpans(ctx context.Context, traces map[otelstorage.TraceID]struct{}) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.spans

	ctx, span := q.tracer.Start(ctx, "QuerySpans",
		trace.WithAttributes(
			attribute.Int("ytstorage.traces_count", len(traces)),
			attribute.Stringer("ytstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	// Then, query all spans for each found trace ID.
	var query strings.Builder
	fmt.Fprintf(&query, "* FROM [%s] WHERE trace_id IN (\n", table)
	n := 0
	for id := range traces {
		if n != 0 {
			query.WriteString(",\n")
		}
		fmt.Fprintf(&query, "\t%q", id)
		n++
	}
	query.WriteString("\n)")

	r, err := q.yc.SelectRows(ctx, query.String(), nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[tracestorage.Span]{reader: r}, nil
}
