package tempohandler

import (
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type metadataCollector struct {
	limit     int
	metadatas map[tracestorage.TraceID]tempoapi.TraceSearchMetadata
}

func (b *metadataCollector) init() {
	if b.metadatas == nil {
		b.metadatas = make(map[tracestorage.TraceID]tempoapi.TraceSearchMetadata)
	}
}

func (b *metadataCollector) AddSpan(span tracestorage.Span) error {
	b.init()

	traceID := span.TraceID
	m, ok := b.metadatas[traceID]
	if !ok {
		if len(b.metadatas) >= b.limit {
			return nil
		}
		m = tempoapi.TraceSearchMetadata{
			TraceID: traceID.Hex(),
			SpanSet: tempoapi.NewOptTempoSpanSet(tempoapi.TempoSpanSet{}),
		}
	}
	ss := &m.SpanSet

	if span.ParentSpanID.IsEmpty() {
		span.FillTraceMetadata(&m)
	}
	ss.Value.Spans = append(ss.Value.Spans, span.AsTempoSpan())

	// Put modified struct back to map.
	b.metadatas[traceID] = m
	return nil
}

func (b *metadataCollector) Result() (r []tempoapi.TraceSearchMetadata) {
	r = make([]tempoapi.TraceSearchMetadata, 0, len(b.metadatas))
	for _, v := range b.metadatas {
		// Skip trace, if we have not got parent span yet.
		if v.StartTimeUnixNano.IsZero() {
			continue
		}
		r = append(r, v)
	}
	return r
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

func (b *batchCollector) getSpanSlice(s tracestorage.Span) ptrace.SpanSlice {
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

func (b *batchCollector) AddSpan(span tracestorage.Span) error {
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
