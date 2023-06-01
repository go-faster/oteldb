package ytstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/traceql"
)

var _ traceql.Span = (*traceqlSpan)(nil)

func otelValueToStatic(val pcommon.Value) (s traceql.Static, _ bool) {
	switch val.Type() {
	case pcommon.ValueTypeStr:
		s.Type = traceql.TypeString
		s.S = val.Str()
	case pcommon.ValueTypeBool:
		s.Type = traceql.TypeBoolean
		s.B = val.Bool()
	case pcommon.ValueTypeInt:
		s.Type = traceql.TypeInt
		s.N = int(val.Int())
	case pcommon.ValueTypeDouble:
		s.Type = traceql.TypeFloat
		s.F = val.Double()
	case pcommon.ValueTypeBytes:
		s.Type = traceql.TypeString
		s.S = val.AsString()
	default:
		return s, false
	}
	return s, true
}

type traceqlSpan struct {
	span  Span
	attrs map[traceql.Attribute]traceql.Static
}

func (s *traceqlSpan) Attributes() map[traceql.Attribute]traceql.Static {
	return s.attrs
}

func (s *traceqlSpan) ID() [8]byte {
	return s.span.SpanID
}

func (s *traceqlSpan) StartTimeUnixNanos() uint64 {
	return s.span.Start
}

func (s *traceqlSpan) DurationNanos() uint64 {
	return s.span.End - s.span.Start
}

func ytToTraceQLSpan(span Span) traceql.Span {
	attrs := span.Attrs.AsMap()
	s := &traceqlSpan{
		span:  span,
		attrs: make(map[traceql.Attribute]traceql.Static, attrs.Len()),
	}
	attrs.Range(func(k string, v pcommon.Value) bool {
		attr := traceql.Attribute{
			Scope: traceql.AttributeScopeSpan,
			Name:  k,
		}

		val, ok := otelValueToStatic(v)
		if !ok {
			// Keep iterating.
			return true
		}
		s.attrs[attr] = val

		return true
	})
	return s
}

func ytToTraceQLSpansetAttribute(to []*traceql.SpansetAttribute, attrs Attrs) []*traceql.SpansetAttribute {
	attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
		s, ok := otelValueToStatic(v)
		if !ok {
			// Keep iterating.
			return true
		}
		to = append(to, &traceql.SpansetAttribute{
			Name: k,
			Val:  s,
		})
		return true
	})
	return to
}

type conditionCollector struct {
	logicOp     string
	query       *strings.Builder
	needLogicOp bool
}

func (c *conditionCollector) addQueryCondition(cond traceql.Condition) error {
	if c.needLogicOp {
		c.query.WriteByte(' ')
		c.query.WriteString(c.logicOp)
		c.query.WriteByte(' ')
	} else {
		c.needLogicOp = true
	}
	switch cond.Op {
	case traceql.OpEqual:
	case traceql.OpNotEqual:
	case traceql.OpGreater:
	case traceql.OpGreaterEqual:
	case traceql.OpLess:
	case traceql.OpLessEqual:
	case traceql.OpRegex:
	case traceql.OpNotRegex:
	default:
		return c.justRetrive(cond)
	}
	return c.justRetrive(cond)
}

func (c *conditionCollector) justRetrive(cond traceql.Condition) error {
	hasYsonAttr := func(sb *strings.Builder, scope, name string) {
		yp := "/" + name
		fmt.Fprintf(sb, "not is_null(try_get_any(%s, %q))", scope, yp)
	}
	attr := cond.Attribute
	switch scope := attr.Scope; scope {
	case traceql.AttributeScopeNone:
		switch intrinsic := attr.Intrinsic; intrinsic {
		case traceql.IntrinsicNone:
			c.query.WriteByte('(')
			hasYsonAttr(c.query, "attrs", cond.Attribute.Name)
			c.query.WriteString(" or ")
			hasYsonAttr(c.query, "resource_attrs", cond.Attribute.Name)
			c.query.WriteByte(')')
		case traceql.IntrinsicDuration,
			traceql.IntrinsicName,
			traceql.IntrinsicStatus,
			traceql.IntrinsicKind,
			traceql.IntrinsicTraceRootService,
			traceql.IntrinsicTraceRootSpan,
			traceql.IntrinsicTraceDuration,
			traceql.IntrinsicTraceID,
			traceql.IntrinsicTraceStartTime,
			traceql.IntrinsicSpanID,
			traceql.IntrinsicSpanStartTime:
			// Present in span anyway.
		default:
			return errors.Errorf("unsupported intrinsic: %q", intrinsic)
		}
	case traceql.AttributeScopeResource:
		hasYsonAttr(c.query, "resource_attrs", cond.Attribute.Name)
	case traceql.AttributeScopeSpan:
		hasYsonAttr(c.query, "attrs", cond.Attribute.Name)
	default:
		return errors.Errorf("unsupported scope: %q", scope)
	}
	return nil
}

func (h *TempoAPI) executeQL(ctx context.Context, params tempoapi.SearchParams) (map[TraceID]tempoapi.TraceSearchMetadata, error) {
	pipeline, err := traceql.ParseQuery(params.Q.Value)
	if err != nil {
		return nil, errors.Wrap(err, "parse query")
	}
	req := traceql.CreateFetchSpansRequest(params, pipeline)

	var query strings.Builder
	fmt.Fprintf(&query, "* from [%s] where true", h.tables.spans)
	if s, ok := params.Start.Get(); ok {
		n := s.UnixNano()
		fmt.Fprintf(&query, " and start >= %d", n)
	}
	if s, ok := params.End.Get(); ok {
		n := s.UnixNano()
		fmt.Fprintf(&query, " and end <= %d", n)
	}
	if d, ok := params.MinDuration.Get(); ok {
		n := d.Nanoseconds()
		fmt.Fprintf(&query, " and (end-start) >= %d", n)
	}
	if d, ok := params.MaxDuration.Get(); ok {
		n := d.Nanoseconds()
		fmt.Fprintf(&query, " and (end-start) <= %d", n)
	}

	query.WriteString(" and (\n")
	op := "or"
	if req.AllConditions {
		op = "and"
	}
	c := conditionCollector{
		logicOp: op,
		query:   &query,
	}
	for _, cond := range req.Conditions {
		if err := c.addQueryCondition(cond); err != nil {
			return nil, errors.Wrap(err, "add condition")
		}
	}
	query.WriteString("\n)")

	zctx.From(ctx).Debug("Execute TraceQL query",
		zap.String("traceql", params.Q.Value),
		zap.Stringer("query", &query),
	)
	spansets := map[TraceID]*traceql.Spanset{}
	if err := h.querySpans(ctx, query.String(), func(s Span) error {
		traceID := s.TraceID

		spanset, ok := spansets[traceID]
		if !ok {
			spanset = new(traceql.Spanset)
			spansets[traceID] = spanset
		}

		if s.ParentSpanID.IsEmpty() {
			spanset.TraceID = s.TraceID
			spanset.RootSpanName = s.Name
			if attr, ok := pcommon.Map(s.ResourceAttrs).Get("service.name"); ok {
				spanset.RootServiceName = attr.AsString()
			}
			spanset.StartTimeUnixNanos = s.Start
			spanset.DurationNanos = s.End - s.Start
			spanset.Attributes = ytToTraceQLSpansetAttribute(spanset.Attributes, s.ScopeAttrs)
			spanset.Attributes = ytToTraceQLSpansetAttribute(spanset.Attributes, s.ResourceAttrs)
		}
		spanset.Spans = append(spanset.Spans, ytToTraceQLSpan(s))

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "query spans")
	}

	metadatas := map[TraceID]tempoapi.TraceSearchMetadata{}
	limit := params.Limit.Or(20)
outer:
	for traceID, in := range spansets {
		eval, err := traceql.EvaluatePipeline(in, pipeline)
		if err != nil {
			return nil, errors.Wrap(err, "evaluate")
		}

		for _, ss := range eval {
			metadatas[traceID] = ss.AsTraceSearchMetadata()
			if len(metadatas) >= limit {
				break outer
			}
		}
	}
	return metadatas, nil
}
