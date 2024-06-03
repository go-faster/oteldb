package traceql

import (
	"fmt"
	"strconv"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// SpanMatcher defines span predicate to select.
type SpanMatcher struct {
	Attribute Attribute
	Op        BinaryOp // could be zero, look for spans with such attribute
	Static    Static
}

// String implements [fmt.Stringer].
func (m SpanMatcher) String() string {
	var static string
	switch s := m.Static; s.Type {
	case TypeString:
		static = strconv.Quote(s.AsString())
	case TypeInt:
		static = strconv.FormatInt(s.AsInt(), 10)
	case TypeNumber:
		static = strconv.FormatFloat(s.AsNumber(), 'f', -1, 64)
	case TypeBool:
		static = strconv.FormatBool(s.AsBool())
	case TypeNil:
		static = "nil"
	case TypeDuration:
		static = s.AsDuration().String()
	case TypeSpanStatus:
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
	case TypeSpanKind:
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
