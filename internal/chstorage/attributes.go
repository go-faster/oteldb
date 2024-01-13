package chstorage

import (
	"fmt"

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

func encodeAttributesRow(attrs pcommon.Map, ck, cv proto.ColumnOf[[]string], ct proto.ColumnOf[[]uint8]) {
	var (
		keys   []string
		values []string
		types  []uint8
	)
	attrs.Range(func(k string, v pcommon.Value) bool {
		s, t := valueToStrType(v)
		keys = append(keys, k)
		values = append(values, s)
		types = append(types, t)
		return true
	})
	ck.Append(keys)
	cv.Append(values)
	ct.Append(types)
}

type Attributes struct {
	Name   string
	Keys   *proto.ColArr[string]
	Values *proto.ColArr[string]
	Types  *proto.ColArr[uint8]
	Hash   proto.ColRawOf[otelstorage.Hash]
}

// NewAttributes constructs a new Attributes storage representation.
func NewAttributes(name string) *Attributes {
	return &Attributes{
		Name: name,

		Keys:   new(proto.ColStr).Array(),
		Values: new(proto.ColStr).Array(),
		Types:  new(proto.ColUInt8).Array(),
	}
}

const (
	attrKeys   = "keys"
	attrValues = "values"
	attrHash   = "hash"
	attrTypes  = "types"
)

func attrCol(name, col string) string {
	return fmt.Sprintf("%s_%s", name, col)
}

// Columns returns a slice of Columns for this attribute set.
func (a *Attributes) Columns() Columns {
	return Columns{
		{Name: attrCol(a.Name, attrKeys), Data: a.Keys},
		{Name: attrCol(a.Name, attrValues), Data: a.Values},
		{Name: attrCol(a.Name, attrTypes), Data: a.Types},
		{Name: attrCol(a.Name, attrHash), Data: &a.Hash},
	}
}

const (
	colAttrs    = "attribute"
	colResource = "resource"
	colScope    = "scope"
)

func attrSelector(name, key string) string {
	return fmt.Sprintf("%s[indexOf(%s, %s)]",
		attrCol(name, attrValues), attrCol(name, attrKeys), singleQuoted(key),
	)
}

// Append adds a new map of attributes.
func (a *Attributes) Append(kv otelstorage.Attrs) {
	encodeAttributesRow(kv.AsMap(), a.Keys, a.Values, a.Types)
	a.Hash.Append(kv.Hash())
}

// Row returns a new map of attributes for a given row.
func (a *Attributes) Row(idx int) (otelstorage.Attrs, error) {
	var (
		m      = pcommon.NewMap()
		keys   = a.Keys.Row(idx)
		values = a.Values.Row(idx)
		types  = a.Types.Row(idx)
	)
	if len(keys) != len(values) || len(keys) != len(types) {
		return otelstorage.Attrs{}, errors.Errorf("attributes: keys, values and types have different lengths")
	}
	for i := range keys {
		if err := valueFromStrType(m.PutEmpty(keys[i]), values[i], types[i]); err != nil {
			return otelstorage.Attrs{}, errors.Wrap(err, "decode value")
		}
	}
	return otelstorage.Attrs(m), nil
}

func attrsToLabels(m otelstorage.Attrs, to map[string]string) {
	m.AsMap().Range(func(k string, v pcommon.Value) bool {
		to[k] = v.Str()
		return true
	})
}
