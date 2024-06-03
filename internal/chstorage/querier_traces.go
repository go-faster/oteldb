package chstorage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
	"github.com/go-faster/oteldb/internal/xattribute"
)

// SearchTags performs search by given tags.
func (q *Querier) SearchTags(ctx context.Context, tags map[string]string, opts tracestorage.SearchTagsOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.SearchTags",
		trace.WithAttributes(
			xattribute.StringMap("chstorage.tags", tags),
			attribute.Int64("chstorage.range.start", int64(opts.Start)),
			attribute.Int64("chstorage.range.end", int64(opts.End)),
			attribute.Int64("chstorage.min_duration", int64(opts.MinDuration)),
			attribute.Int64("chstorage.max_duration", int64(opts.MaxDuration)),

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
			colAttrs,
			colResource,
			colScope,
		} {
			if i != 0 {
				query.WriteString(" OR ")
			}
			fmt.Fprintf(&query,
				`%s = %s`,
				attrSelector(column, key), singleQuoted(value),
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
func (q *Querier) TagNames(ctx context.Context, opts tracestorage.TagNamesOptions) (r []tracestorage.TagName, rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.TagNames",
		trace.WithAttributes(
			attribute.Stringer("chstorage.scope", opts.Scope),
			attribute.Int64("chstorage.range.start", int64(opts.Start)),
			attribute.Int64("chstorage.range.end", int64(opts.End)),
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
	fmt.Fprintf(&query, "SELECT DISTINCT name, scope FROM %#q", table)
	switch scope := opts.Scope; scope {
	case traceql.ScopeNone:
	case traceql.ScopeResource:
		// Tempo merges scope attributes and resource attributes.
		fmt.Fprintf(&query, " WHERE scope IN (%d, %d)", scope, traceql.ScopeInstrumentation)
	case traceql.ScopeSpan:
		fmt.Fprintf(&query, " WHERE scope = %d", scope)
	default:
		return r, errors.Errorf("unexpected scope %v", scope)
	}

	var (
		name  = new(proto.ColStr).LowCardinality()
		scope proto.ColEnum8
	)
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query.String(),
		Result: proto.Results{
			{Name: "name", Data: name},
			{Name: "scope", Data: proto.Wrap(&scope, scopeTypeDDL)},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < name.Rows(); i++ {
				r = append(r, tracestorage.TagName{
					Name:  name.Row(i),
					Scope: traceql.AttributeScope(scope.Row(i)),
				})
			}
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}
	return r, nil
}

// TagValues returns all available tag values for given tag.
func (q *Querier) TagValues(ctx context.Context, tag traceql.Attribute, opts tracestorage.TagValuesOptions) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	ctx, span := q.tracer.Start(ctx, "chstorage.traces.TagValues",
		trace.WithAttributes(
			attribute.Stringer("chstorage.tag", tag),
			attribute.Int64("chstorage.range.start", int64(opts.Start)),
			attribute.Int64("chstorage.range.end", int64(opts.End)),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	switch tag.Prop {
	case traceql.SpanAttribute:
		return q.attributeValues(ctx, tag, opts)
	case traceql.SpanStatus:
		// TODO(tdakkota): probably we should do a proper query.
		name := tag.String()
		statuses := []tracestorage.Tag{
			{Name: name, Value: "unset", Type: traceql.TypeSpanStatus},
			{Name: name, Value: "ok", Type: traceql.TypeSpanStatus},
			{Name: name, Value: "error", Type: traceql.TypeSpanStatus},
		}
		return iterators.Slice(statuses), nil
	case traceql.SpanKind:
		// TODO(tdakkota): probably we should do a proper query.
		name := tag.String()
		kinds := []tracestorage.Tag{
			{Name: name, Value: "unspecified", Type: traceql.TypeSpanKind},
			{Name: name, Value: "internal", Type: traceql.TypeSpanKind},
			{Name: name, Value: "server", Type: traceql.TypeSpanKind},
			{Name: name, Value: "client", Type: traceql.TypeSpanKind},
			{Name: name, Value: "producer", Type: traceql.TypeSpanKind},
			{Name: name, Value: "consumer", Type: traceql.TypeSpanKind},
		}
		return iterators.Slice(kinds), nil
	case traceql.SpanDuration, traceql.SpanChildCount, traceql.SpanParent, traceql.TraceDuration:
		// Too high cardinality to query.
		return iterators.Empty[tracestorage.Tag](), nil
	case traceql.SpanName, traceql.RootSpanName:
		// FIXME(tdakkota): we don't check if span name is actually coming from a root span.
		return q.spanNames(ctx, tag, opts)
	case traceql.RootServiceName:
		// FIXME(tdakkota): we don't check if service.name actually coming from a root span.
		//
		// Equals to `resource.service.name`.
		tag = traceql.Attribute{Name: "service.name", Scope: traceql.ScopeResource}
		return q.attributeValues(ctx, tag, opts)
	default:
		return nil, errors.Errorf("unexpected span property %v (attribute: %q)", tag.Prop, tag)
	}
}

func (q *Querier) spanNames(ctx context.Context, tag traceql.Attribute, opts tracestorage.TagValuesOptions) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.spanNames",
		trace.WithAttributes(
			attribute.Stringer("chstorage.tag", tag),
			attribute.Int64("chstorage.range.start", int64(opts.Start)),
			attribute.Int64("chstorage.range.end", int64(opts.End)),
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
	fmt.Fprintf(&query, `SELECT DISTINCT name FROM %#q WHERE true`, table)
	if s := opts.Start; s != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(start) >= %d", s)
	}
	if e := opts.End; e != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(end) <= %d", e)
	}

	var (
		tagName = tag.String()

		name = new(proto.ColStr).LowCardinality()
		r    []tracestorage.Tag
	)
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query.String(),
		Result: proto.Results{
			{Name: "name", Data: name},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < name.Rows(); i++ {
				r = append(r, tracestorage.Tag{
					Name:  tagName,
					Value: name.Row(i),
					Type:  traceql.TypeString,
					Scope: traceql.ScopeNone,
				})
			}
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}

	return iterators.Slice(r), nil
}

func (q *Querier) attributeValues(ctx context.Context, tag traceql.Attribute, opts tracestorage.TagValuesOptions) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.attributeValues",
		trace.WithAttributes(
			attribute.Int64("chstorage.range.start", int64(opts.Start)),
			attribute.Int64("chstorage.range.end", int64(opts.End)),
			attribute.Stringer("chstorage.tag", tag),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	// FIXME(tdakkota): respect time range parameters.
	var query strings.Builder
	fmt.Fprintf(&query, `SELECT DISTINCT value, value_type FROM %#q WHERE name = %s`, table, singleQuoted(tag.Name))
	switch scope := tag.Scope; scope {
	case traceql.ScopeNone:
	case traceql.ScopeResource:
		// Tempo merges scope attributes and resource attributes.
		fmt.Fprintf(&query, " AND scope IN (%d, %d)", scope, traceql.ScopeInstrumentation)
	case traceql.ScopeSpan:
		fmt.Fprintf(&query, " AND scope = %d", scope)
	default:
		return nil, errors.Errorf("unexpected scope %v", scope)
	}

	var (
		value     proto.ColStr
		valueType proto.ColEnum8

		r []tracestorage.Tag
	)
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query.String(),
		Result: proto.Results{
			{Name: "value", Data: &value},
			{Name: "value_type", Data: proto.Wrap(&valueType, valueTypeDDL)},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			return value.ForEach(func(i int, value string) error {
				typ := pcommon.ValueType(valueType.Row(i))
				r = append(r, tracestorage.Tag{
					Name:  tag.Name,
					Value: value,
					Type:  traceql.StaticTypeFromValueType(typ),
					Scope: traceql.ScopeNone,
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

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.TraceByID",
		trace.WithAttributes(
			attribute.String("chstorage.id_to_query", id.Hex()),
			attribute.Int64("chstorage.range.start", int64(opts.Start)),
			attribute.Int64("chstorage.range.end", int64(opts.End)),
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

	queryStartTime := time.Now()
	defer func() {
		q.clickhouseRequestHistogram.Record(ctx, time.Since(queryStartTime).Seconds(),
			metric.WithAttributes(
				attribute.String("chstorage.query_type", "TraceByID"),
				attribute.String("chstorage.table", table),
				attribute.String("chstorage.signal", "traces"),
			),
		)
	}()
	return q.querySpans(ctx, query)
}

var _ traceqlengine.Querier = (*Querier)(nil)

// SelectSpansets get spansets from storage.
func (q *Querier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (_ iterators.Iterator[traceqlengine.Trace], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.SelectSpansets",
		trace.WithAttributes(
			attribute.Stringer("traceql.span_matcher_operation", params.Op),
			xattribute.StringerSlice("traceql.matchers", params.Matchers),
			attribute.Int64("traceql.range.start", int64(params.Start)),
			attribute.Int64("traceql.range.end", int64(params.End)),
			attribute.Int64("traceql.min_duration", int64(params.MinDuration)),
			attribute.Int64("traceql.max_duration", int64(params.MaxDuration)),
			attribute.Int("traceql.limit", params.Limit),

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
		queryStartTime = time.Now()
		query          = q.buildSpansetsQuery(table, span, params)
	)
	iter, err := q.querySpans(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "query traces")
	}
	defer func() {
		_ = iter.Close()
	}()
	q.clickhouseRequestHistogram.Record(ctx, time.Since(queryStartTime).Seconds(),
		metric.WithAttributes(
			attribute.String("chstorage.query_type", "SelectSpansets"),
			attribute.String("chstorage.table", table),
			attribute.String("chstorage.signal", "traces"),
		),
	)

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
	span.AddEvent("spans_fetched", trace.WithAttributes(
		attribute.Int("chstorage.queried_spans", spansCount),
		attribute.Int("chstorage.queried_traces", len(result)),
	))

	return iterators.Slice(result), nil
}

func (q *Querier) buildSpansetsQuery(table string, span trace.Span, params traceqlengine.SelectSpansetsParams) string {
	var query strings.Builder

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
					`has(%s, %s)`,
					attrKeys(column), singleQuoted(attr.Name),
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
				fmt.Fprintf(&query, "service_namespace %s %s", cmp, singleQuoted(value))
			case semconv.ServiceNameKey:
				fmt.Fprintf(&query, "service_name %s %s", cmp, singleQuoted(value))
			case semconv.ServiceInstanceIDKey:
				fmt.Fprintf(&query, "service_instance_id %s %s", cmp, singleQuoted(value))
			default:
				for i, column := range getTraceQLAttributeColumns(attr) {
					if i != 0 {
						query.WriteString("\nOR ")
					}
					fmt.Fprintf(&query, "%s %s %s",
						attrSelector(column, attr.Name),
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
			colAttrs,
			colResource,
			colScope,
		}
	case traceql.ScopeResource:
		return []string{
			colScope,
			colResource,
		}
	case traceql.ScopeSpan:
		return []string{
			colAttrs,
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
