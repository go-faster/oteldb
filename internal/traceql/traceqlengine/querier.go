package traceqlengine

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

// Trace is set of span grouped by trace ID.
type Trace struct {
	TraceID otelstorage.TraceID
	Spans   []tracestorage.Span
}

// Querier does queries to storage.
type Querier interface {
	// SelectSpansets get spansets from storage.
	SelectSpansets(ctx context.Context, params SelectSpansetsParams) (iterators.Iterator[Trace], error)
}

// SpanMatcher defines span predicate to select.
type SpanMatcher struct {
	Attribute traceql.Attribute
	Op        traceql.BinaryOp // could be zero, look for spans with such attribute
	Static    traceql.Static
}

// String implements [fmt.Stringer].
func (m SpanMatcher) String() string {
	var static string
	switch s := m.Static; s.Type {
	case traceql.TypeString:
		static = strconv.Quote(s.AsString())
	case traceql.TypeInt:
		static = strconv.FormatInt(s.AsInt(), 10)
	case traceql.TypeNumber:
		static = strconv.FormatFloat(s.AsNumber(), 'f', -1, 64)
	case traceql.TypeBool:
		static = strconv.FormatBool(s.AsBool())
	case traceql.TypeNil:
		static = "nil"
	case traceql.TypeDuration:
		static = s.AsDuration().String()
	case traceql.TypeSpanStatus:
		switch status := s.AsSpanStatus(); status {
		case ptrace.StatusCodeUnset:
			static = "unset"
		case ptrace.StatusCodeOk:
			static = "ok"
		case ptrace.StatusCodeError:
			static = "error"
		default:
			static = fmt.Sprintf("<invalid span status: %#v>", status)
		}
	case traceql.TypeSpanKind:
		switch Kind := s.AsSpanKind(); Kind {
		case ptrace.SpanKindUnspecified:
			static = "unspecified"
		case ptrace.SpanKindInternal:
			static = "internal"
		case ptrace.SpanKindServer:
			static = "server"
		case ptrace.SpanKindClient:
			static = "client"
		case ptrace.SpanKindProducer:
			static = "producer"
		case ptrace.SpanKindConsumer:
			static = "consumer"
		default:
			static = fmt.Sprintf("<invalid span kind: %#v>", Kind)
		}
	default:
		static = fmt.Sprintf("<invalid static: %#v>", s)
	}
	return fmt.Sprintf("%s %s %s", m.Attribute, m.Op, static)
}

// SelectSpansetsParams is a storage query params.
type SelectSpansetsParams struct {
	Op       traceql.SpansetOp // OpAnd, OpOr
	Matchers []SpanMatcher

	// Time range to query, optional.
	Start, End otelstorage.Timestamp

	// Trace duration, querier should ignore field, if it is zero.
	// TODO(tdakkota): probably, we can put it as SpanMatcher with traceDuration attribute
	//	but it would not work properly with OpOr.
	MinDuration time.Duration
	MaxDuration time.Duration

	Limit int
}

// MemoryQuerier is a simple in-memory querier, used for tests.
type MemoryQuerier struct {
	data map[otelstorage.TraceID][]tracestorage.Span
}

// SelectSpansets get spansets from storage.
func (q *MemoryQuerier) SelectSpansets(context.Context, SelectSpansetsParams) (iterators.Iterator[Trace], error) {
	var result []Trace
	for traceID, spans := range q.data {
		result = append(result, Trace{
			TraceID: traceID,
			Spans:   spans,
		})
	}
	return iterators.Slice(result), nil
}

// Add adds span to data set.
//
// NOTE: There is no synchronization. Do not call this function concurrently with other methods.
func (q *MemoryQuerier) Add(span tracestorage.Span) {
	if q.data == nil {
		q.data = map[otelstorage.TraceID][]tracestorage.Span{}
	}
	q.data[span.TraceID] = append(q.data[span.TraceID], span)
}
