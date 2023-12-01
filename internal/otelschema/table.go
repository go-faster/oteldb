package otelschema

import (
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Appendable is a column that can append values.
type Appendable interface {
	AppendAttribute(v pcommon.Value)
	AppendZero()
}

// AppendableColumn is a column that can append values.
type AppendableColumn interface {
	Appendable
	proto.Column
}

// TypedColumn is a column that can append values of a given type.
type TypedColumn[T any] struct {
	proto.ColumnOf[T]
	Append func(col proto.ColumnOf[T], v pcommon.Value)
}

// AppendAttribute appends an attribute to the column.
func (t *TypedColumn[T]) AppendAttribute(v pcommon.Value) {
	t.Append(t.ColumnOf, v)
}

// AppendZero appends a zero value to the column.
func (t *TypedColumn[T]) AppendZero() {
	var v T
	t.ColumnOf.Append(v)
}

// AttributeColumn is a column with a normalized name.
type AttributeColumn struct {
	Key    string
	Name   string
	Column AppendableColumn
}

// Table is a table with normalized column names.
type Table struct {
	encoder jx.Encoder

	// Raw json.
	Raw proto.ColStr
	// Columns normalized.
	Columns []AttributeColumn
}

// Reset resets table columns.
func (t *Table) Reset() {
	t.encoder.Reset()
	t.Raw.Reset()
	for _, c := range t.Columns {
		c.Column.Reset()
	}
}

// Append appends attributes to the table.
func (t *Table) Append(attrs otelstorage.Attrs) {
	kv := attrs.AsMap()
	t.encoder.Reset()

	appendZeroes := make(map[string]struct{}, len(t.Columns))
	for _, c := range t.Columns {
		appendZeroes[c.Key] = struct{}{}
	}

	t.encoder.ObjStart()
	kv.Range(func(k string, v pcommon.Value) bool {
		for _, c := range t.Columns {
			if c.Key == k {
				c.Column.AppendAttribute(v)
				delete(appendZeroes, k)
				return true
			}
		}
		t.encoder.Field(k, func(e *jx.Encoder) {
			switch v.Type() {
			case pcommon.ValueTypeStr:
				e.Str(v.Str())
			case pcommon.ValueTypeInt:
				e.Int64(v.Int())
			default:
				e.Null()
			}
		})
		return true
	})
	t.encoder.ObjEnd()
	t.Raw.AppendBytes(t.encoder.Bytes())
	for _, c := range t.Columns {
		if _, ok := appendZeroes[c.Key]; !ok {
			continue
		}
		c.Column.AppendZero()
	}
}

func newTypedColumn(t proto.ColumnType) AppendableColumn {
	switch t {
	case proto.ColumnTypeString:
		return &TypedColumn[string]{
			ColumnOf: new(proto.ColStr),
			Append:   func(col proto.ColumnOf[string], v pcommon.Value) { col.Append(v.Str()) },
		}
	case proto.ColumnTypeInt64:
		return &TypedColumn[int64]{
			ColumnOf: new(proto.ColInt64),
			Append:   func(col proto.ColumnOf[int64], v pcommon.Value) { col.Append(v.Int()) },
		}
	default:
		panic("unknown column type")
	}
}

// Input returns input for a ClickHouse query.
func (t *Table) Input() proto.Input {
	out := proto.Input{
		{
			Name: "raw",
			Data: &t.Raw,
		},
	}
	for _, c := range t.Columns {
		out = append(out, proto.InputColumn{
			Name: c.Name,
			Data: c.Column,
		})
	}
	return out
}

// AttributeInfo describes a column.
type AttributeInfo struct {
	Name string // col.name
	Type proto.ColumnType
}

// NewTable creates a new table with normalized column names.
func NewTable(columns []AttributeInfo) *Table {
	t := &Table{
		Raw: proto.ColStr{},
	}
	for _, c := range columns {
		mappedName := strings.ReplaceAll(c.Name, ".", "_")
		t.Columns = append(t.Columns, AttributeColumn{
			Key:    c.Name,
			Name:   mappedName,
			Column: newTypedColumn(c.Type),
		})
	}
	return t
}
