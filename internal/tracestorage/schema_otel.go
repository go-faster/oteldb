package tracestorage

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// NewSpanFromOTEL creates new Span.
func NewSpanFromOTEL(
	batchID string,
	res pcommon.Resource,
	scope pcommon.InstrumentationScope,
	span ptrace.Span,
) (s Span) {
	status := span.Status()
	s = Span{
		TraceID:       TraceID(span.TraceID()),
		SpanID:        SpanID(span.SpanID()),
		TraceState:    span.TraceState().AsRaw(),
		ParentSpanID:  SpanID(span.ParentSpanID()),
		Name:          span.Name(),
		Kind:          int32(span.Kind()),
		Start:         uint64(span.StartTimestamp()),
		End:           uint64(span.EndTimestamp()),
		Attrs:         Attrs(span.Attributes()),
		StatusCode:    int32(status.Code()),
		StatusMessage: status.Message(),
		BatchID:       batchID,
		ResourceAttrs: Attrs(res.Attributes()),
		ScopeName:     scope.Name(),
		ScopeVersion:  scope.Version(),
		ScopeAttrs:    Attrs(scope.Attributes()),
	}

	return s
}

// FillOTELSpan fills given OpenTelemetry span using span fields.
func (span Span) FillOTELSpan(s ptrace.Span) {
	// FIXME(tdakkota): probably, we can just implement YSON (en/de)coder for UUID.
	s.SetTraceID(pcommon.TraceID(span.TraceID))
	s.SetSpanID(pcommon.SpanID(span.SpanID))
	s.TraceState().FromRaw(span.TraceState)
	if p := span.ParentSpanID; !p.IsEmpty() {
		s.SetParentSpanID(pcommon.SpanID(p))
	}
	s.SetName(span.Name)
	s.SetKind(ptrace.SpanKind(span.Kind))
	s.SetStartTimestamp(pcommon.Timestamp(span.Start))
	s.SetEndTimestamp(pcommon.Timestamp(span.End))
	span.Attrs.CopyTo(s.Attributes())

	status := s.Status()
	status.SetCode(ptrace.StatusCode(span.StatusCode))
	status.SetMessage(span.StatusMessage)
}
