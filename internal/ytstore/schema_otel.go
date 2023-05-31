package ytstore

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

type spanKey struct {
	batchID      string
	scopeName    string
	scopeVersion string
}

type batchCollector struct {
	traces     ptrace.Traces
	resSpans   map[string]ptrace.ResourceSpans
	scopeSpans map[spanKey]ptrace.SpanSlice
}

func (b *batchCollector) init() {
	var zeroTraces ptrace.Traces
	if b.traces == zeroTraces {
		b.traces = ptrace.NewTraces()
	}
	if b.resSpans == nil {
		b.resSpans = make(map[string]ptrace.ResourceSpans)
	}
	if b.scopeSpans == nil {
		b.scopeSpans = make(map[spanKey]ptrace.SpanSlice)
	}
}

func (b *batchCollector) getSpanSlice(s Span) ptrace.SpanSlice {
	b.init()

	k := spanKey{
		batchID:      s.BatchID,
		scopeName:    s.ScopeName,
		scopeVersion: s.ScopeVersion,
	}

	ss, ok := b.scopeSpans[k]
	if ok {
		return ss
	}

	resSpan, ok := b.resSpans[s.BatchID]
	if !ok {
		resSpan = b.traces.ResourceSpans().AppendEmpty()
		b.resSpans[s.BatchID] = resSpan
	}
	res := resSpan.Resource()
	s.ResourceAttrs.CopyTo(res.Attributes())

	scopeSpan := resSpan.ScopeSpans().AppendEmpty()
	scope := scopeSpan.Scope()
	scope.SetName(s.ScopeName)
	scope.SetVersion(s.ScopeVersion)
	s.ScopeAttrs.CopyTo(scope.Attributes())

	ss = scopeSpan.Spans()
	b.scopeSpans[k] = ss
	return ss
}

func (b *batchCollector) AddSpan(span Span) error {
	s := b.getSpanSlice(span).AppendEmpty()
	span.FillOTELSpan(s)
	return nil
}

func (b *batchCollector) Result() ptrace.Traces {
	var zeroTraces ptrace.Traces
	if b.traces == zeroTraces {
		b.traces = ptrace.NewTraces()
	}
	return b.traces
}
