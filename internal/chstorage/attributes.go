package chstorage

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

type Attributes struct {
	Name  string
	Value proto.ColumnOf[otelstorage.Attrs]
}

type attributeCol struct {
	index  *proto.ColBytes
	col    *proto.ColLowCardinalityRaw
	hashes map[otelstorage.Hash]int

	// values are filled up only when decoding.
	values []otelstorage.Attrs
}

func (a attributeCol) Type() proto.ColumnType {
	return proto.ColumnTypeLowCardinality.Sub(proto.ColumnTypeString)
}

func (a attributeCol) Rows() int {
	return a.col.Rows()
}

func (a *attributeCol) DecodeColumn(r *proto.Reader, rows int) error {
	if err := a.col.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "col")
	}
	for i := 0; i < a.index.Rows(); i++ {
		v := a.index.Row(i)
		m, err := decodeAttributes(v)
		if err != nil {
			return errors.Wrapf(err, "index value %d", i)
		}
		a.hashes[m.Hash()] = i
		a.values = append(a.values, m)
	}
	return nil
}

func (a *attributeCol) Reset() {
	a.col.Reset()
	a.index.Reset()
	a.values = a.values[:0]
	maps.Clear(a.hashes)
	a.col.Key = proto.KeyUInt64
}

func (a *attributeCol) EncodeColumn(b *proto.Buffer) {
	a.col.EncodeColumn(b)
}

func (a *attributeCol) Append(v otelstorage.Attrs) {
	a.col.Key = proto.KeyUInt64
	h := v.Hash()
	idx, ok := a.hashes[h]
	if !ok {
		idx = len(a.hashes)
		a.hashes[h] = idx

		e := jx.GetEncoder()
		defer jx.PutEncoder(e)
		encodeMap(e, v.AsMap())

		// Append will copy passed bytes.
		a.index.Append(e.Bytes())
	}
	a.col.AppendKey(idx)
}

func (a *attributeCol) AppendArr(v []otelstorage.Attrs) {
	for _, m := range v {
		a.Append(m)
	}
}

func (a *attributeCol) DecodeState(r *proto.Reader) error {
	return a.col.DecodeState(r)
}

func (a *attributeCol) EncodeState(b *proto.Buffer) {
	a.col.EncodeState(b)
}

func (a attributeCol) rowIdx(i int) int {
	switch a.col.Key {
	case proto.KeyUInt8:
		return int(a.col.Keys8[i])
	case proto.KeyUInt16:
		return int(a.col.Keys16[i])
	case proto.KeyUInt32:
		return int(a.col.Keys32[i])
	case proto.KeyUInt64:
		return int(a.col.Keys64[i])
	default:
		panic("invalid key type")
	}
}

func (a attributeCol) Row(i int) otelstorage.Attrs {
	return a.values[a.rowIdx(i)]
}

func newAttributesColumn() proto.ColumnOf[otelstorage.Attrs] {
	ac := &attributeCol{
		index:  new(proto.ColBytes),
		hashes: map[otelstorage.Hash]int{},
	}
	ac.col = &proto.ColLowCardinalityRaw{
		Index: ac.index,
		Key:   proto.KeyUInt64,
	}
	return ac
}

// NewAttributes constructs a new Attributes storage representation.
func NewAttributes(name string) *Attributes {
	return &Attributes{
		Name:  name,
		Value: newAttributesColumn(),
	}
}

func attrKeys(name string) string {
	return fmt.Sprintf("JSONExtractKeys(%s)", name)
}

// Columns returns a slice of Columns for this attribute set.
func (a *Attributes) Columns() Columns {
	return Columns{
		{Name: a.Name, Data: a.Value},
	}
}

const (
	colAttrs    = "attribute"
	colResource = "resource"
	colScope    = "scope"
)

func attrSelector(name, key string) string {
	return fmt.Sprintf("JSONExtractString(%s, %s)",
		name, singleQuoted(key),
	)
}

// Append adds a new map of attributes.
func (a *Attributes) Append(kv otelstorage.Attrs) {
	a.Value.Append(kv)
}

// Row returns a new map of attributes for a given row.
func (a *Attributes) Row(idx int) otelstorage.Attrs {
	return a.Value.Row(idx)
}

func attrsToLabels(m otelstorage.Attrs, to map[string]string) {
	m.AsMap().Range(func(k string, v pcommon.Value) bool {
		to[k] = v.Str()
		return true
	})
}
