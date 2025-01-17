package chstorage

import (
	"sort"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/ddl"
	"github.com/go-faster/oteldb/internal/otelschema"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type Attributes struct {
	Name  string
	Value proto.ColumnOf[otelstorage.Attrs]

	// Materialized columns.

	Strings  map[string]proto.ColumnOf[string]
	Integers map[string]proto.ColumnOf[int64]
	UUIDs    map[string]proto.ColumnOf[uuid.UUID]

	columnToAttr map[string]string
	attrToColumn map[string]string
}

type jsonAttrCol struct {
	col *proto.ColStr

	// values are filled up only when decoding.
	values []otelstorage.Attrs
}

func (a jsonAttrCol) Type() proto.ColumnType {
	return proto.ColumnTypeString
}

func (a jsonAttrCol) Rows() int {
	return a.col.Rows()
}

func (a *jsonAttrCol) DecodeColumn(r *proto.Reader, rows int) error {
	if err := a.col.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "col")
	}
	for i := 0; i < a.col.Rows(); i++ {
		v := a.col.RowBytes(i)
		m, err := decodeAttributes(v)
		if err != nil {
			return errors.Wrapf(err, "index value %d", i)
		}
		a.values = append(a.values, m)
	}
	return nil
}

func (a *jsonAttrCol) Reset() {
	a.col.Reset()
	a.values = a.values[:0]
}

func (a *jsonAttrCol) EncodeColumn(b *proto.Buffer) {
	a.col.EncodeColumn(b)
}

func (a *jsonAttrCol) WriteColumn(w *proto.Writer) {
	a.col.WriteColumn(w)
}

func (a *jsonAttrCol) Append(v otelstorage.Attrs) {
	e := jx.GetEncoder()
	defer jx.PutEncoder(e)
	encodeMap(e, v.AsMap())

	// Append will copy passed bytes.
	a.col.AppendBytes(e.Bytes())
}

func (a *jsonAttrCol) AppendArr(v []otelstorage.Attrs) {
	for _, m := range v {
		a.Append(m)
	}
}

func (a jsonAttrCol) Row(i int) otelstorage.Attrs {
	return a.values[i]
}

type jsonLowCardinalityAttrCol struct {
	index  *proto.ColBytes
	col    *proto.ColLowCardinalityRaw
	hashes map[otelstorage.Hash]int

	// values are filled up only when decoding.
	values []otelstorage.Attrs
}

func (a jsonLowCardinalityAttrCol) Type() proto.ColumnType {
	return proto.ColumnTypeLowCardinality.Sub(proto.ColumnTypeString)
}

func (a jsonLowCardinalityAttrCol) Rows() int {
	return a.col.Rows()
}

func (a *jsonLowCardinalityAttrCol) DecodeColumn(r *proto.Reader, rows int) error {
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

func (a *jsonLowCardinalityAttrCol) Reset() {
	a.col.Reset()
	a.index.Reset()
	a.values = a.values[:0]
	maps.Clear(a.hashes)
	a.col.Key = proto.KeyUInt64
}

func (a *jsonLowCardinalityAttrCol) EncodeColumn(b *proto.Buffer) {
	a.col.EncodeColumn(b)
}

func (a *jsonLowCardinalityAttrCol) WriteColumn(w *proto.Writer) {
	a.col.WriteColumn(w)
}

func (a *jsonLowCardinalityAttrCol) Append(v otelstorage.Attrs) {
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

func (a *jsonLowCardinalityAttrCol) AppendArr(v []otelstorage.Attrs) {
	for _, m := range v {
		a.Append(m)
	}
}

func (a *jsonLowCardinalityAttrCol) DecodeState(r *proto.Reader) error {
	return a.col.DecodeState(r)
}

func (a *jsonLowCardinalityAttrCol) EncodeState(b *proto.Buffer) {
	a.col.EncodeState(b)
}

func (a jsonLowCardinalityAttrCol) rowIdx(i int) int {
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

func (a jsonLowCardinalityAttrCol) Row(i int) otelstorage.Attrs {
	return a.values[a.rowIdx(i)]
}

func newAttributesColumn(opt attributesOptions) proto.ColumnOf[otelstorage.Attrs] {
	if !opt.LowCardinality {
		return &jsonAttrCol{
			col: new(proto.ColStr),
		}
	}
	ac := &jsonLowCardinalityAttrCol{
		index:  new(proto.ColBytes),
		hashes: map[otelstorage.Hash]int{},
	}
	ac.col = &proto.ColLowCardinalityRaw{
		Index: ac.index,
		Key:   proto.KeyUInt64,
	}
	return ac
}

type attributesOptions struct {
	LowCardinality bool
}

type AttributesOption func(*attributesOptions)

func WithLowCardinality(v bool) AttributesOption {
	return func(o *attributesOptions) {
		o.LowCardinality = v
	}
}

// NewAttributes constructs a new Attributes storage representation.
func NewAttributes(name string, opts ...AttributesOption) *Attributes {
	o := attributesOptions{
		LowCardinality: true,
	}
	for _, opt := range opts {
		opt(&o)
	}
	attr := &Attributes{
		Name:  name,
		Value: newAttributesColumn(o),

		Strings:  make(map[string]proto.ColumnOf[string]),
		Integers: make(map[string]proto.ColumnOf[int64]),
		UUIDs:    make(map[string]proto.ColumnOf[uuid.UUID]),

		columnToAttr: make(map[string]string),
		attrToColumn: make(map[string]string),
	}

	appendEntry := func(e otelschema.Entry, prefix string) {
		switch e.Name {
		case "service_name", "service_namespace", "service_instance_id":
			// Already materialized by hand.
			return
		}
		s := prefix + e.Name
		attr.columnToAttr[s] = e.FullName
		attr.attrToColumn[e.FullName] = s
		switch e.Type {
		case "string":
			if strings.HasPrefix(e.Column.String(), "Enum") {
				v := new(proto.ColEnum)
				if err := v.Infer(e.Column); err != nil {
					panic(err)
				}
				attr.Strings[s] = v
				return
			}
			if e.Column == proto.ColumnTypeString {
				attr.Strings[s] = new(proto.ColStr)
				if name == colResource {
					attr.Strings[s] = new(proto.ColStr).LowCardinality()
				}
			}
			if e.Column == proto.ColumnTypeUUID {
				attr.UUIDs[s] = new(proto.ColUUID)
			}
			return
		case "int":
			if e.Column != proto.ColumnTypeInt64 {
				// TODO: support other columns?
				return
			}
			attr.Integers[s] = new(proto.ColInt64)
		}
	}

	type variant struct {
		Prefix string
		Where  otelschema.Where
	}
	if v, ok := map[string]variant{
		colAttrs:    {"attr_", otelschema.WhereAttribute},
		colResource: {"res_", otelschema.WhereResource},
		colScope:    {"scp_", otelschema.WhereScope},
	}[name]; ok {
		for _, e := range otelschema.Data.All()[:0] {
			if e.WhereIn(v.Where) {
				prefix := v.Prefix
				if len(e.Where) == 1 {
					prefix = ""
				}
				appendEntry(e, prefix)
			}
		}
	}

	return attr
}

func attrKeys(name string) chsql.Expr {
	return chsql.JSONExtractKeys(chsql.Ident(name))
}

func attrStringMap(name string) chsql.Expr {
	return chsql.JSONExtract(chsql.Ident(name), "Map(String, String)")
}

// Columns returns a slice of Columns for this attribute set.
func (a *Attributes) Columns() Columns {
	col := Columns{
		{Name: a.Name, Data: a.Value},
	}
	col = appendColumns(col, a.Strings)
	col = appendColumns(col, a.Integers)
	return col
}

func appendColumns[T any](col Columns, m map[string]proto.ColumnOf[T]) Columns {
	keys := maps.Keys(m)
	sort.Strings(keys)
	for _, k := range keys {
		col = append(col, Column{
			Name: k,
			Data: m[k],
		})
	}
	return col
}

const (
	colAttrs    = "attribute"
	colResource = "resource"
	colScope    = "scope"
)

func attrSelector(name, key string) chsql.Expr {
	return chsql.JSONExtractString(chsql.Ident(name), key)
}

func firstAttrSelector(label string) chsql.Expr {
	columns := make([]chsql.Expr, 0, 4)
	for _, column := range []string{
		colAttrs,
		colScope,
		colResource,
	} {
		columns = append(columns, chsql.JSONExtractField(
			chsql.Ident(column),
			label,
			"Nullable(String)",
		))
	}
	columns = append(columns, chsql.String(""))
	return chsql.Coalesce(columns...)
}

// Append adds a new map of attributes.
func (a *Attributes) Append(kv otelstorage.Attrs) {
	a.Value.Append(kv)

	materializedFieldSet := map[string]struct{}{}
	kv.AsMap().Range(func(k string, v pcommon.Value) bool {
		name, ok := a.attrToColumn[k]
		if !ok {
			return true
		}

		if c, found := a.Strings[name]; found {
			c.Append(v.Str())
			materializedFieldSet[name] = struct{}{}
			return true
		}
		if c, found := a.Integers[name]; found {
			c.Append(v.Int())
			materializedFieldSet[name] = struct{}{}
			return true
		}
		if c, found := a.UUIDs[name]; found {
			s, err := uuid.Parse(v.Str())
			if err != nil {
				s = uuid.Nil
			}
			c.Append(s)
			materializedFieldSet[name] = struct{}{}
		}

		return true
	})

	appendZeroValuesIfNotSet(a.Strings, materializedFieldSet)
	appendZeroValuesIfNotSet(a.Integers, materializedFieldSet)
	appendZeroValuesIfNotSet(a.UUIDs, materializedFieldSet)
}

func appendZeroValuesIfNotSet[T any](m map[string]proto.ColumnOf[T], set map[string]struct{}) {
	for k, v := range m {
		if _, ok := set[k]; !ok {
			var zero T
			v.Append(zero)
		}
	}
}

// Row returns a new map of attributes for a given row.
func (a *Attributes) Row(idx int) otelstorage.Attrs {
	return a.Value.Row(idx)
}

// DDL applies the schema changes to the table.
func (a *Attributes) DDL(table *ddl.Table) {
	table.Columns = append(table.Columns,
		ddl.Column{
			Comment: a.Name + " attributes",
		},
		ddl.Column{
			Name: a.Name,
			Type: a.Value.Type(),
		},
	)
	table.Columns = appendDDL(table.Columns, a.Integers)
	table.Columns = appendDDL(table.Columns, a.Strings)
	table.Columns = appendDDL(table.Columns, a.UUIDs)
	table.Columns = append(table.Columns, ddl.Column{
		Comment: "end",
	})
}

func appendDDL[T any](col []ddl.Column, m map[string]proto.ColumnOf[T]) []ddl.Column {
	keys := maps.Keys(m)
	sort.Strings(keys)
	for _, k := range keys {
		col = append(col, ddl.Column{
			Name: k,
			Type: m[k].Type(),
		})
	}
	return col
}

func attrsToLabels(m otelstorage.Attrs, to map[string]string) {
	m.AsMap().Range(func(k string, v pcommon.Value) bool {
		to[otelstorage.KeyToLabel(k)] = v.Str()
		return true
	})
}
