package promapi

// NewV creates a new V.
func NewV(t float64, v string) V {
	return V{
		Timestamp: t,
		Value:     v,
	}
}

// V is a Value representation.
type V struct {
	Timestamp float64 // 1435781430.781
	Value     string  // "0"
}

// ToValue converts V to Value.
func (v V) ToValue() Value {
	return Value{
		NewFloat64ValueItem(v.Timestamp),
		NewStringValueItem(v.Value),
	}
}

// ToV converts Value to V.
func (v Value) ToV() V {
	if len(v) != 2 || v[0].Type != Float64ValueItem || v[1].Type != StringValueItem {
		panic("invalid value")
	}
	return V{
		Timestamp: (v)[0].Float64,
		Value:     (v)[1].String,
	}
}
