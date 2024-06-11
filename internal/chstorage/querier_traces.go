package chstorage

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
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
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			xattribute.Duration("chstorage.min_duration", opts.MinDuration),
			xattribute.Duration("chstorage.max_duration", opts.MaxDuration),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	subquery := chsql.Select(table, chsql.Column("trace_id", nil)).
		Distinct(true).
		Where(traceInTimeRange(opts.Start, opts.End))
	{
		durationExpr := chsql.Ident("duration_ns")
		if d := opts.MinDuration; d != 0 {
			subquery.Where(
				chsql.Gte(durationExpr, chsql.Integer(int64(d))),
			)
		}
		if d := opts.MaxDuration; d != 0 {
			subquery.Where(
				chsql.Lte(durationExpr, chsql.Integer(int64(d))),
			)
		}
	}
	for key, value := range tags {
		if key == "name" {
			subquery.Where(
				chsql.ColumnEq("name", value),
			)
			continue
		}

		exprs := make([]chsql.Expr, 0, 3)
		for _, column := range []string{
			colAttrs,
			colResource,
			colScope,
		} {
			exprs = append(exprs, chsql.Eq(
				attrSelector(column, key),
				chsql.String(value),
			))
		}
		subquery.Where(chsql.JoinOr(exprs...))
	}

	var (
		c     = newSpanColumns()
		query = chsql.Select(table, c.ChsqlResult()...).
			Where(chsql.In(
				chsql.Ident("trace_id"),
				chsql.SubQuery(subquery),
			))

		r []tracestorage.Span
	)
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) (err error) {
			r, err = c.ReadRowsTo(r)
			return err
		},

		Type:   "SearchTags",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(r), nil
}

// TagNames returns all available tag names.
func (q *Querier) TagNames(ctx context.Context, opts tracestorage.TagNamesOptions) (r []tracestorage.TagName, rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.TagNames",
		trace.WithAttributes(
			attribute.Stringer("chstorage.scope", opts.Scope),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
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
		name  = new(proto.ColStr).LowCardinality()
		scope proto.ColEnum8

		query = chsql.Select(table,
			chsql.Column("name", name),
			chsql.Column("scope", &scope),
		).
			Distinct(true)
	)
	switch scope := opts.Scope; scope {
	case traceql.ScopeNone:
	case traceql.ScopeResource:
		// Tempo merges scope attributes and resource attributes.
		query.Where(
			chsql.In(
				chsql.Ident("scope"),
				chsql.TupleValues(int(scope), int(traceql.ScopeInstrumentation)),
			),
		)
	case traceql.ScopeSpan:
		query.Where(
			chsql.In(
				chsql.Ident("scope"),
				chsql.Integer(int(scope)),
			),
		)
	default:
		return r, errors.Errorf("unexpected scope %v", scope)
	}

	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < name.Rows(); i++ {
				r = append(r, tracestorage.TagName{
					Name:  name.Row(i),
					Scope: traceql.AttributeScope(scope.Row(i)),
				})
			}
			return nil
		},

		Type:   "TagNames",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return r, nil
}

// TagValues returns all available tag values for given tag.
func (q *Querier) TagValues(ctx context.Context, tag traceql.Attribute, opts tracestorage.TagValuesOptions) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	ctx, span := q.tracer.Start(ctx, "chstorage.traces.TagValues",
		trace.WithAttributes(
			attribute.Stringer("chstorage.tag", tag),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			attribute.Stringer("traceql.autocomplete", opts.AutocompleteQuery),
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
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),

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
		name  = new(proto.ColStr).LowCardinality()
		query = chsql.Select(table, chsql.Column("name", name)).
			Distinct(true).
			Where(traceInTimeRange(opts.Start, opts.End))

		tagName = tag.String()
		r       []tracestorage.Tag
	)
	if err := q.do(ctx, selectQuery{
		Query: query,
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

		Type:   "SpanNames",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(r), nil
}

func (q *Querier) attributeValues(ctx context.Context, tag traceql.Attribute, opts tracestorage.TagValuesOptions) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.attributeValues",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
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
	var (
		value     proto.ColStr
		valueType proto.ColEnum8

		query = chsql.Select(table,
			chsql.Column("value", &value),
			chsql.Column("value_type", proto.Wrap(&valueType, valueTypeDDL)),
		).
			Distinct(true).
			Where(chsql.ColumnEq("name", tag.Name))
	)
	switch scope := tag.Scope; scope {
	case traceql.ScopeNone:
	case traceql.ScopeResource:
		// Tempo merges scope attributes and resource attributes.
		query.Where(
			chsql.In(
				chsql.Ident("scope"),
				chsql.TupleValues(int(scope), int(traceql.ScopeInstrumentation)),
			),
		)
	case traceql.ScopeSpan:
		query.Where(
			chsql.In(
				chsql.Ident("scope"),
				chsql.Integer(int(scope)),
			),
		)
	default:
		return nil, errors.Errorf("unexpected scope %v", scope)
	}

	var r []tracestorage.Tag
	if err := q.do(ctx, selectQuery{
		Query: query,
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

		Type:   "attributeValues",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(r), nil
}

// TraceByID returns spans of given trace.
func (q *Querier) TraceByID(ctx context.Context, id otelstorage.TraceID, opts tracestorage.TraceByIDOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.TraceByID",
		trace.WithAttributes(
			attribute.String("chstorage.id_to_query", id.Hex()),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),

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
		c     = newSpanColumns()
		query = chsql.Select(table, c.ChsqlResult()...).
			Where(
				chsql.Eq(
					chsql.Ident("trace_id"),
					chsql.Unhex(chsql.String(id.Hex())),
				),
				traceInTimeRange(opts.Start, opts.End),
			)
	)

	var r []tracestorage.Span
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) (err error) {
			r, err = c.ReadRowsTo(r)
			return err
		},

		Type:   "TraceByID",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(r), nil
}

var _ traceqlengine.Querier = (*Querier)(nil)

// SelectSpansets get spansets from storage.
func (q *Querier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (_ iterators.Iterator[traceqlengine.Trace], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.SelectSpansets",
		trace.WithAttributes(
			attribute.Stringer("traceql.span_matcher_operation", params.Op),
			xattribute.StringerSlice("traceql.matchers", params.Matchers),
			xattribute.UnixNano("traceql.range.start", params.Start),
			xattribute.UnixNano("traceql.range.end", params.End),
			xattribute.Duration("traceql.min_duration", params.MinDuration),
			xattribute.Duration("traceql.max_duration", params.MaxDuration),
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
		c     = newSpanColumns()
		query = chsql.Select(table, c.ChsqlResult()...).
			Where(
				chsql.In(
					chsql.Ident("trace_id"),
					chsql.SubQuery(q.buildSpansetsQuery(table, span, params)),
				),
			).
			Order(chsql.Ident("start"), chsql.Asc)

		traces = map[otelstorage.TraceID][]tracestorage.Span{}
	)
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.traceID.Rows(); i++ {
				span, err := c.Row(i)
				if err != nil {
					return err
				}
				traces[span.TraceID] = append(traces[span.TraceID], span)
			}
			return nil
		},

		Type:   "SelectSpansets",
		Signal: "traces",
		Table:  table,
	}); err != nil {
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
		attribute.Int("chstorage.total_spans", spansCount),
		attribute.Int("chstorage.total_traces", len(result)),
	))

	return iterators.Slice(result), nil
}

func (q *Querier) buildSpansetsQuery(table string, span trace.Span, params traceqlengine.SelectSpansetsParams) *chsql.SelectQuery {
	var (
		dropped    int
		matchExprs = make([]chsql.Expr, 0, len(params.Matchers))
	)
	for _, matcher := range params.Matchers {
		if matcher.Op == 0 {
			// Just query spans with this attribute.
			var (
				attr  = matcher.Attribute
				exprs = make([]chsql.Expr, 0, 3)
			)
			for _, column := range getTraceQLAttributeColumns(attr) {
				exprs = append(exprs, chsql.Has(
					attrKeys(column),
					chsql.String(attr.Name),
				))
			}
			if len(exprs) > 0 {
				matchExprs = append(matchExprs, chsql.JoinOr(exprs...))
			}
			continue
		}

		op, ok := getTraceQLMatcherOp(matcher.Op)
		if !ok {
			// Unsupported for now.
			dropped++
			continue
		}

		value, ok := getTraceQLLiteral(matcher.Static)
		if !ok {
			// Unsupported for now.
			dropped++
			continue
		}

		switch attr := matcher.Attribute; attr.Prop {
		case traceql.SpanDuration:
			matchExprs = append(matchExprs, op(
				chsql.Ident("duration_ns"),
				value,
			))
		case traceql.SpanName:
			matchExprs = append(matchExprs, op(
				chsql.Ident("name"),
				value,
			))
		case traceql.SpanStatus:
			matchExprs = append(matchExprs, op(
				chsql.Ident("status_code"),
				value,
			))
		case traceql.SpanKind:
			matchExprs = append(matchExprs, op(
				chsql.Ident("kind"),
				value,
			))
		case traceql.SpanParent,
			traceql.SpanChildCount,
			traceql.RootSpanName,
			traceql.RootServiceName,
			traceql.TraceDuration:
			// Unsupported yet.
			dropped++
			continue
		default:
			// SpanAttribute
			switch attribute.Key(attr.Name) {
			case semconv.ServiceNamespaceKey:
				matchExprs = append(matchExprs, op(
					chsql.Ident("service_namespace"),
					value,
				))
			case semconv.ServiceNameKey:
				matchExprs = append(matchExprs, op(
					chsql.Ident("service_name"),
					value,
				))
			case semconv.ServiceInstanceIDKey:
				matchExprs = append(matchExprs, op(
					chsql.Ident("service_instance_id"),
					value,
				))
			default:
				exprs := make([]chsql.Expr, 0, 3)
				for _, column := range getTraceQLAttributeColumns(attr) {
					exprs = append(exprs, op(
						attrSelector(column, attr.Name),
						chsql.ToString(value),
					))
				}
				if len(exprs) > 0 {
					matchExprs = append(matchExprs, chsql.JoinOr(exprs...))
				}
			}
		}
	}
	span.SetAttributes(
		attribute.Int("chstorage.unsupported_span_matchers", dropped),
		attribute.String("chstorage.table", table),
	)

	query := chsql.Select(table,
		chsql.Column("trace_id", nil)).
		Distinct(true).
		Where(traceInTimeRange(params.Start, params.End))

	if len(matchExprs) > 0 {
		if params.Op == traceql.SpansetOpAnd {
			query.Where(matchExprs...)
		} else {
			query.Where(chsql.JoinOr(matchExprs...))
		}
	}
	return query
}

func traceInTimeRange(start, end time.Time) chsql.Expr {
	exprs := make([]chsql.Expr, 0, 2)
	if !start.IsZero() {
		exprs = append(exprs, chsql.Gte(
			chsql.ToUnixTimestamp64Nano(chsql.Ident("start")),
			chsql.UnixNano(start),
		))
	}
	if !end.IsZero() {
		exprs = append(exprs, chsql.Lte(
			chsql.ToUnixTimestamp64Nano(chsql.Ident("end")),
			chsql.UnixNano(end),
		))
	}
	return chsql.JoinAnd(exprs...)
}

func getTraceQLLiteral(s traceql.Static) (value chsql.Expr, _ bool) {
	switch s.Type {
	case traceql.TypeString:
		return chsql.String(s.AsString()), true
	case traceql.TypeInt:
		return chsql.Integer(s.AsInt()), true
	case traceql.TypeNumber:
		return chsql.Float(s.AsNumber()), true
	case traceql.TypeBool:
		return chsql.Bool(s.AsBool()), true
	case traceql.TypeDuration:
		return chsql.Integer(s.AsDuration().Nanoseconds()), true
	case traceql.TypeSpanStatus:
		return chsql.Integer(int(s.AsSpanStatus())), true
	case traceql.TypeSpanKind:
		return chsql.Integer(int(s.AsSpanKind())), true
	default:
		return value, false
	}
}

func getTraceQLMatcherOp(op traceql.BinaryOp) (func(l, r chsql.Expr) chsql.Expr, bool) {
	switch op {
	case traceql.OpEq:
		return chsql.Eq, true
	case traceql.OpNotEq:
		return chsql.NotEq, true
	case traceql.OpGt:
		return chsql.Gt, true
	case traceql.OpGte:
		return chsql.Gte, true
	case traceql.OpLt:
		return chsql.Lt, true
	case traceql.OpLte:
		return chsql.Lte, true
	case traceql.OpRe:
		return chsql.Match, true
	default:
		return nil, false
	}
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
