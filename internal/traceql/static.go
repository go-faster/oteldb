package traceql

import (
	"math"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// StaticType defines static type.
type StaticType int

const (
	StaticString StaticType = iota + 1
	StaticInteger
	StaticNumber
	StaticBool
	StaticNil
	StaticDuration
	StaticSpanStatus
	StaticSpanKind
)

// Static is a constant value.
type Static struct {
	Type StaticType
	Data uint64 // stores everything, except strings
	Str  string
}

func (s *Static) resetTo(typ StaticType) {
	s.Type = typ
	s.Data = 0
	s.Str = ""
}

// SetString sets String value.
func (s *Static) SetString(v string) {
	s.resetTo(StaticString)
	s.Str = v
}

// SetInteger sets Integer value.
func (s *Static) SetInteger(v int64) {
	s.resetTo(StaticInteger)
	s.Data = uint64(v)
}

// SetNumber sets Number value.
func (s *Static) SetNumber(v float64) {
	s.resetTo(StaticNumber)
	s.Data = math.Float64bits(v)
}

// SetBool sets Bool value.
func (s *Static) SetBool(v bool) {
	s.resetTo(StaticBool)
	if v {
		s.Data = 1
	}
}

// SetNil sets Nil value.
func (s *Static) SetNil() {
	s.resetTo(StaticNil)
}

// SetDuration sets Duration value.
func (s *Static) SetDuration(v time.Duration) {
	s.resetTo(StaticDuration)
	s.Data = uint64(v)
}

// SetSpanStatus sets SpanStatus value.
func (s *Static) SetSpanStatus(status ptrace.StatusCode) {
	s.resetTo(StaticSpanStatus)
	s.Data = uint64(status)
}

// SetSpanKind sets SpanKind value.
func (s *Static) SetSpanKind(kind ptrace.SpanKind) {
	s.resetTo(StaticSpanKind)
	s.Data = uint64(kind)
}

// AsString returns String value.
func (s *Static) AsString() string {
	return s.Str
}

// AsInteger returns Integer value.
func (s *Static) AsInteger() int64 {
	return int64(s.Data)
}

// AsNumber returns Number value.
func (s *Static) AsNumber() float64 {
	return math.Float64frombits(s.Data)
}

// AsBool returns Bool value.
func (s *Static) AsBool() bool {
	return s.Data != 0
}

// IsNil returns true, if static is Nil.
func (s *Static) IsNil() bool {
	return s.Type == StaticNil
}

// AsDuration returns Duration value.
func (s *Static) AsDuration() time.Duration {
	return time.Duration(s.Data)
}

// AsSpanStatus returns SpanStatus value.
func (s *Static) AsSpanStatus() ptrace.StatusCode {
	return ptrace.StatusCode(s.Data)
}

// AsSpanKind returns SpanKind value.
func (s *Static) AsSpanKind() ptrace.SpanKind {
	return ptrace.SpanKind(s.Data)
}
