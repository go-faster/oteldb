package traceql

import (
	"fmt"
	"math"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// StaticType defines static type.
type StaticType int

const (
	TypeAttribute StaticType = iota
	TypeString
	TypeInt
	TypeNumber
	TypeBool
	TypeNil
	TypeDuration
	TypeSpanStatus
	TypeSpanKind
	lastType
)

// String implements fmt.Stringer.
func (s StaticType) String() string {
	switch s {
	case TypeAttribute:
		return "Attribute"
	case TypeString:
		return "String"
	case TypeInt:
		return "Int"
	case TypeNumber:
		return "Number"
	case TypeBool:
		return "Bool"
	case TypeNil:
		return "Nil"
	case TypeDuration:
		return "Duration"
	case TypeSpanStatus:
		return "SpanStatus"
	case TypeSpanKind:
		return "SpanKind"
	default:
		return fmt.Sprintf("<unknown type %d>", s)
	}
}

// CheckOperand whether is a and b are valid operands.
func (s StaticType) CheckOperand(s2 StaticType) bool {
	return s == s2 ||
		s == TypeAttribute || s2 == TypeAttribute ||
		(s.IsNumeric() && s2.IsNumeric())
}

// IsNumeric returns true if type is numeric.
func (s StaticType) IsNumeric() bool {
	switch s {
	case TypeInt, TypeNumber, TypeDuration:
		return true
	default:
		return false
	}
}

// Static is a constant value.
type Static struct {
	Type StaticType
	Data uint64 // stores everything, except strings
	Str  string
}

// ValueType returns value type of expression.
func (s Static) ValueType() StaticType {
	return s.Type
}

func (s *Static) resetTo(typ StaticType) {
	s.Type = typ
	s.Data = 0
	s.Str = ""
}

// SetString sets String value.
func (s *Static) SetString(v string) {
	s.resetTo(TypeString)
	s.Str = v
}

// SetInt sets Int value.
func (s *Static) SetInt(v int64) {
	s.resetTo(TypeInt)
	s.Data = uint64(v)
}

// SetNumber sets Number value.
func (s *Static) SetNumber(v float64) {
	s.resetTo(TypeNumber)
	s.Data = math.Float64bits(v)
}

// SetBool sets Bool value.
func (s *Static) SetBool(v bool) {
	s.resetTo(TypeBool)
	if v {
		s.Data = 1
	}
}

// SetNil sets Nil value.
func (s *Static) SetNil() {
	s.resetTo(TypeNil)
}

// SetDuration sets Duration value.
func (s *Static) SetDuration(v time.Duration) {
	s.resetTo(TypeDuration)
	s.Data = uint64(v)
}

// SetSpanStatus sets SpanStatus value.
func (s *Static) SetSpanStatus(status ptrace.StatusCode) {
	s.resetTo(TypeSpanStatus)
	s.Data = uint64(status)
}

// SetSpanKind sets SpanKind value.
func (s *Static) SetSpanKind(kind ptrace.SpanKind) {
	s.resetTo(TypeSpanKind)
	s.Data = uint64(kind)
}

// AsString returns String value.
func (s *Static) AsString() string {
	return s.Str
}

// AsInt returns Int value.
func (s *Static) AsInt() int64 {
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
	return s.Type == TypeNil
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
