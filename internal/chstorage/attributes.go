package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

func valueToStrType(v pcommon.Value) (string, uint8) {
	if t := v.Type(); t == pcommon.ValueTypeStr {
		return v.Str(), uint8(t)
	}
	e := &jx.Encoder{}
	encodeValue(e, v)
	return e.String(), uint8(v.Type())
}

func valueFromStrType(v pcommon.Value, s string, t uint8) error {
	if t := pcommon.ValueType(t); t == pcommon.ValueTypeStr {
		v.SetStr(s)
		return nil
	}
	d := jx.DecodeStr(s)
	return decodeValue(d, v)
}

func encodeAttributesRow(attrs pcommon.Map) ([]attrKV, []typKV) {
	out := make([]attrKV, 0, attrs.Len())
	outTypes := make([]typKV, 0, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		s, t := valueToStrType(v)
		out = append(out, proto.KV[string, string]{
			Key:   k,
			Value: s,
		})
		outTypes = append(outTypes, proto.KV[string, uint8]{
			Key:   k,
			Value: t,
		})
		return true
	})
	return out, outTypes
}

func newAttributesColumn() *proto.ColMap[string, string] {
	return proto.NewMap[string, string](
		new(proto.ColStr).LowCardinality(),
		new(proto.ColStr),
	)
}

func newTypesColumn() *proto.ColMap[string, uint8] {
	return proto.NewMap[string, uint8](
		new(proto.ColStr).LowCardinality(),
		new(proto.ColUInt8),
	)
}

type Attributes struct {
	Name  string
	Data  *proto.ColMap[string, string]
	Types *proto.ColMap[string, uint8]
}

type (
	attrKV = proto.KV[string, string]
	typKV  = proto.KV[string, uint8]
)

// NewAttributes constructs a new Attributes storage representation.
func NewAttributes(name string) *Attributes {
	return &Attributes{
		Name:  name,
		Data:  newAttributesColumn(),
		Types: newTypesColumn(),
	}
}

// Columns returns a slice of Columns for this attribute set.
func (a *Attributes) Columns() Columns {
	return Columns{
		{Name: a.Name, Data: a.Data},
		{Name: a.Name + "_types", Data: a.Types},
	}
}

// Append adds a new map of attributes.
func (a *Attributes) Append(kv otelstorage.Attrs) {
	va, vt := encodeAttributesRow(kv.AsMap())
	a.Data.AppendKV(va)
	a.Types.AppendKV(vt)
}

// Row returns a new map of attributes for a given row.
func (a *Attributes) Row(idx int) (otelstorage.Attrs, error) {
	m := pcommon.NewMap()
	s := a.Data.RowKV(idx)
	st := a.Types.RowKV(idx)
	if len(s) != len(st) {
		return otelstorage.Attrs{}, errors.New("length mismatch")
	}
	for i, kv := range s {
		t := st[i].Value
		v := m.PutEmpty(kv.Key)
		if err := valueFromStrType(v, kv.Value, t); err != nil {
			return otelstorage.Attrs{}, errors.Wrap(err, "decode value")
		}
	}
	return otelstorage.Attrs(m), nil
}
