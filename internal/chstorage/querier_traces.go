package chstorage

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

// SearchTags performs search by given tags.
func (q *Querier) SearchTags(ctx context.Context, tags map[string]string, opts tracestorage.SearchTagsOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "SearchTags",
		trace.WithAttributes(
			attribute.Int("chstorage.tags_count", len(tags)),
			attribute.Int64("chstorage.start_range", int64(opts.Start)),
			attribute.Int64("chstorage.end_range", int64(opts.End)),
			attribute.Int64("chstorage.max_duration", int64(opts.MaxDuration)),
			attribute.Int64("chstorage.min_duration", int64(opts.MinDuration)),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var query strings.Builder
	fmt.Fprintf(&query, `SELECT %s FROM %#[2]q WHERE trace_id IN (
		SELECT DISTINCT trace_id FROM %#[2]q WHERE true
	`, newSpanColumns().columns().All(), table)
	for key, value := range tags {
		if key == "name" {
			fmt.Fprintf(&query, " AND name = %s", singleQuoted(value))
			continue
		}

		query.WriteString(" AND (")
		for i, column := range []string{
			"attributes",
			"scope_attributes",
			"resource",
		} {
			if i != 0 {
				query.WriteString(" OR ")
			}
			fmt.Fprintf(&query,
				`%s[%s] = %s`,
				column, singleQuoted(key), singleQuoted(value),
			)
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
func (q *Querier) TagNames(ctx context.Context) (r []string, rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "TagNames",
		trace.WithAttributes(
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	data := new(proto.ColStr).LowCardinality()
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT DISTINCT name FROM %#q", table),
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
func (q *Querier) TagValues(ctx context.Context, tagName string) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "TagValues",
		trace.WithAttributes(
			attribute.String("chstorage.tag_to_query", tagName),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		value     proto.ColStr
		valueType proto.ColEnum8

		r []tracestorage.Tag
	)

	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT DISTINCT value, value_type FROM %#q WHERE name = %s", table, singleQuoted(tagName)),
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

	return iterators.Slice(r), nil
}

// TraceByID returns spans of given trace.
func (q *Querier) TraceByID(ctx context.Context, id otelstorage.TraceID, opts tracestorage.TraceByIDOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "TraceByID",
		trace.WithAttributes(
			attribute.String("chstorage.id_to_query", id.Hex()),
			attribute.Int64("chstorage.start_range", int64(opts.Start)),
			attribute.Int64("chstorage.end_range", int64(opts.End)),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	query := fmt.Sprintf("SELECT %s FROM %#q WHERE trace_id = unhex(%s)",
		newSpanColumns().columns().All(), table, singleQuoted(id.Hex()),
	)
	if s := opts.Start; s != 0 {
		query += fmt.Sprintf(" AND toUnixTimestamp64Nano(start) >= %d", s)
	}
	if e := opts.End; e != 0 {
		query += fmt.Sprintf(" AND toUnixTimestamp64Nano(end) <= %d", e)
	}
	return q.querySpans(ctx, query)
}

var _ traceqlengine.Querier = (*Querier)(nil)

// SelectSpansets get spansets from storage.
func (q *Querier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (_ iterators.Iterator[traceqlengine.Trace], rerr error) {
	ctx, span := q.tracer.Start(ctx, "SelectSpansets",
		trace.WithAttributes(
			attribute.String("chstorage.span_matcher_operation", params.Op.String()),
			attribute.Int("chstorage.span_matchers", len(params.Matchers)),
			attribute.Int64("chstorage.start_range", int64(params.Start)),
			attribute.Int64("chstorage.end_range", int64(params.End)),
			attribute.Int64("chstorage.max_duration", int64(params.MaxDuration)),
			attribute.Int64("chstorage.min_duration", int64(params.MinDuration)),
			attribute.Int("chstorage.limit", params.Limit),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	query := q.buildSpansetsQuery(span, params)
	zctx.From(ctx).Debug("Query", zap.String("query", query))

	iter, err := q.querySpans(ctx, query)
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
		attribute.Int("chstorage.queried_spans", spansCount),
		attribute.Int("chstorage.queried_traces", len(result)),
	)

	return iterators.Slice(result), nil
}

func (q *Querier) buildSpansetsQuery(span trace.Span, params traceqlengine.SelectSpansetsParams) string {
	var (
		query strings.Builder
		table = q.tables.Spans
	)

	fmt.Fprintf(&query, `SELECT %s FROM %#[2]q WHERE trace_id IN (
		SELECT DISTINCT trace_id FROM %#[2]q WHERE true
	`, newSpanColumns().columns().All(), table)

	var (
		dropped   int
		writeNext = func() {
			if params.Op == traceql.SpansetOpAnd {
				query.WriteString("\nAND ")
			} else {
				query.WriteString("\nOR ")
			}
		}
	)
	for _, matcher := range params.Matchers {
		if matcher.Op == 0 {
			writeNext()

			// Just query spans with this attribute.
			attr := matcher.Attribute
			query.WriteString("(\n")
			for i, column := range getTraceQLAttributeColumns(attr) {
				if i != 0 {
					query.WriteString(" OR ")
				}
				fmt.Fprintf(&query,
					`mapContains(%s, %s) = 1`,
					column, singleQuoted(attr.Name),
				)
				query.WriteByte('\n')
			}
			query.WriteString("\n)")
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
		case traceql.OpRe:
			cmp = "REGEXP"
		default:
			// Unsupported for now.
			dropped++
			continue
		}

		var value, typeName string
		switch s := matcher.Static; s.Type {
		case traceql.TypeString:
			value = s.Str
			typeName = "String"
		case traceql.TypeInt:
			value = strconv.FormatInt(s.AsInt(), 10)
			typeName = "Int64"
		case traceql.TypeNumber:
			value = strconv.FormatFloat(s.AsNumber(), 'f', -1, 64)
			typeName = "Float64"
		case traceql.TypeBool:
			if s.AsBool() {
				value = "true"
			} else {
				value = "false"
			}
			typeName = "Boolean"
		case traceql.TypeDuration:
			value = strconv.FormatInt(s.AsDuration().Nanoseconds(), 10)
			typeName = "Int64"
		case traceql.TypeSpanStatus:
			value = strconv.Itoa(int(s.AsSpanStatus()))
			typeName = "Int64"
		case traceql.TypeSpanKind:
			value = strconv.Itoa(int(s.AsSpanKind()))
			typeName = "Int64"
		default:
			// Unsupported for now.
			dropped++
			continue
		}

		{
			_ = typeName
			// TODO(ernado): use it with "_types".
		}

		writeNext()
		switch attr := matcher.Attribute; attr.Prop {
		case traceql.SpanDuration:
			fmt.Fprintf(&query, "duration_ns %s %s", cmp, value)
		case traceql.SpanName:
			fmt.Fprintf(&query, "name %s %s", cmp, singleQuoted(value))
		case traceql.SpanStatus:
			fmt.Fprintf(&query, "status_code %s %s", cmp, singleQuoted(value))
		case traceql.SpanKind:
			fmt.Fprintf(&query, "kind %s %s", cmp, singleQuoted(value))
		case traceql.SpanParent,
			traceql.SpanChildCount,
			traceql.RootSpanName,
			traceql.RootServiceName,
			traceql.TraceDuration:
			// Unsupported yet.
			dropped++
			query.WriteString("true")
		default:
			// SpanAttribute
			query.WriteString("(\n")
			switch attribute.Key(attr.Name) {
			case semconv.ServiceNamespaceKey:
				fmt.Fprintf(&query, "service.namespace %s %s", cmp, singleQuoted(value))
			case semconv.ServiceNameKey:
				fmt.Fprintf(&query, "service_name %s %s", cmp, singleQuoted(value))
			case semconv.ServiceInstanceIDKey:
				fmt.Fprintf(&query, "service_instance_id %s %s", cmp, singleQuoted(value))
			default:
				for i, column := range getTraceQLAttributeColumns(attr) {
					if i != 0 {
						query.WriteString("\nOR ")
					}
					fmt.Fprintf(&query, "%s[%s] %s %s",
						column, singleQuoted(attr.Name),
						cmp, singleQuoted(value),
					)
				}
			}
			query.WriteString("\n)")
		}
	}
	query.WriteString("\n)")
	if s := params.Start; s != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(start) >= %d", s)
	}
	if e := params.End; e != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(end) <= %d", e)
	}
	span.SetAttributes(
		attribute.Int("chstorage.unsupported_span_matchers", dropped),
		attribute.String("chstorage.table", table),
	)
	return query.String()
}

func getTraceQLAttributeColumns(attr traceql.Attribute) []string {
	if attr.Prop != traceql.SpanAttribute || attr.Parent {
		return nil
	}
	switch attr.Scope {
	case traceql.ScopeNone:
		return []string{
			"attributes",
			"scope_attributes",
			"resource",
		}
	case traceql.ScopeResource:
		return []string{
			"scope_attributes",
			"resource",
		}
	case traceql.ScopeSpan:
		return []string{
			"attributes",
		}
	default:
		return nil
	}
}

func (q *Querier) querySpans(ctx context.Context, query string) (iterators.Iterator[tracestorage.Span], error) {
	c := newSpanColumns()

	var r []tracestorage.Span
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query,
		Result: c.Result(),
		OnResult: func(ctx context.Context, block proto.Block) (err error) {
			r, err = c.ReadRowsTo(r)
			return err
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}

	return iterators.Slice(r), nil
}
