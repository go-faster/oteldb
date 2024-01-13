package chstorage

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

type Attributes struct {
	Name  string
	Value proto.ColBytes
}

// NewAttributes constructs a new Attributes storage representation.
func NewAttributes(name string) *Attributes {
	return &Attributes{
		Name: name,
	}
}

func attrKeys(name string) string {
	return fmt.Sprintf("JSONExtractKeys(%s)", name)
}

// Columns returns a slice of Columns for this attribute set.
func (a *Attributes) Columns() Columns {
	return Columns{
		{Name: a.Name, Data: &a.Value},
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
	a.Value.Append(encodeAttributes(kv.AsMap()))
}

// Row returns a new map of attributes for a given row.
func (a *Attributes) Row(idx int) (otelstorage.Attrs, error) {
	return decodeAttributes(a.Value.Row(idx))
}

func attrsToLabels(m otelstorage.Attrs, to map[string]string) {
	m.AsMap().Range(func(k string, v pcommon.Value) bool {
		to[k] = v.Str()
		return true
	})
}
