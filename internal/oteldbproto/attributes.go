package oteldbproto

import (
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Resource describes telemetry resource.
type Resource struct {
	SchemaURL string

	Attributes             Attributes
	DroppedAttributesCount uint32
}

// Scope describes teelemetry scope.
type Scope struct {
	SchemaURL string

	Name                   string
	Version                string
	Attributes             Attributes
	DroppedAttributesCount uint32
}

// Attributes contains a set of attributes.
type Attributes struct {
	Keys   []string
	Values []Value
}

// Append appends pair to attributes.
func (a *Attributes) Append(k string, v Value) {
	a.Keys = append(a.Keys, k)
	a.Values = append(a.Values, v)
}

// KeyValue is a named OpenTelemetry attribute value.
type KeyValue struct {
	Name  string
	Value Value
}

// Value is an OpenTelemetry attribute value.
type Value struct {
	kind uint8
	val  uint64
	// TODO(tdakkota): replace with unsafe.Pointer and use val as length.
	strval string
}

// Kind returns value kind.
func (v Value) Kind() pcommon.ValueType {
	return pcommon.ValueType(v.kind)
}

func (v *Value) setKind(kind pcommon.ValueType) {
	v.kind = uint8(kind)
}

func (v Value) is(kind pcommon.ValueType) bool {
	return v.Kind() == kind
}

// AsStr unpacks the value to string using given string map.
func (v Value) AsStr() (string, bool) {
	if !v.is(pcommon.ValueTypeStr) {
		return "", false
	}
	return v.strval, true
}

// AsInt unpacks the value to int.
func (v Value) AsInt() (val int64, ok bool) {
	val = int64(v.val)
	ok = v.is(pcommon.ValueTypeInt)
	return val, ok
}

// AsDouble unpacks the value to double.
func (v Value) AsDouble() (val float64, ok bool) {
	val = math.Float64frombits(v.val)
	ok = v.is(pcommon.ValueTypeDouble)
	return val, ok
}

// AsBool unpacks the value to bool.
func (v Value) AsBool() (val, ok bool) {
	val = v.val != 0
	ok = v.is(pcommon.ValueTypeBool)
	return val, ok
}

// AsMap unpacks the value to map.
func (v Value) AsMap(pool [][]KeyValue) ([]KeyValue, bool) {
	if !v.is(pcommon.ValueTypeMap) {
		return nil, false
	}

	idx := int(v.val)
	if idx < 0 || idx >= len(pool) {
		return nil, false
	}
	val := pool[idx]
	return val, true
}

// AsSlice unpacks the value to slice.
func (v Value) AsSlice(pool [][]Value) ([]Value, bool) {
	if !v.is(pcommon.ValueTypeSlice) {
		return nil, false
	}

	idx := int(v.val)
	if idx < 0 || idx >= len(pool) {
		return nil, false
	}
	val := pool[idx]
	return val, true
}

// AsBytes unpacks the value to bytes using given string map.
func (v Value) AsBytes() ([]byte, bool) {
	if !v.is(pcommon.ValueTypeBytes) {
		return nil, false
	}
	return []byte(v.strval), true
}

// SetStr sets the value to string using given string map.
func (v *Value) SetStr(s string) {
	v.setKind(pcommon.ValueTypeStr)
	v.strval = s
}

// SetInt sets the value to int.
func (v *Value) SetInt(val int64) {
	v.setKind(pcommon.ValueTypeInt)
	v.val = uint64(val)
}

// SetDouble sets the value to double.
func (v *Value) SetDouble(val float64) {
	v.setKind(pcommon.ValueTypeDouble)
	v.val = math.Float64bits(val)
}

// SetBool sets the value to bool.
func (v *Value) SetBool(val bool) {
	v.setKind(pcommon.ValueTypeBool)
	if val {
		v.val = 1
	}
}

// SetMap sets the value to map.
func (v *Value) SetMap(pool *[][]KeyValue, val []KeyValue) {
	v.setKind(pcommon.ValueTypeMap)
	*pool = append(*pool, val)
	v.val = uint64(len(*pool) - 1)
}

// SetSlice sets the value to slice.
func (v *Value) SetSlice(pool *[][]Value, val []Value) {
	v.setKind(pcommon.ValueTypeSlice)
	*pool = append(*pool, val)
	v.val = uint64(len(*pool) - 1)
}

// SetBytes sets the value to bytes using given string map.
func (v *Value) SetBytes(s []byte) {
	v.setKind(pcommon.ValueTypeBytes)
	v.strval = string(s)
}
