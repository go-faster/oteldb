package otelstorage

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// KeyToLabel converts key to label name.
func KeyToLabel(key string) string {
	return strings.ReplaceAll(key, ".", "_")
}

// Attrs wraps attributes.
type Attrs pcommon.Map

// AsMap returns Attrs as [pcommon.Map].
func (m Attrs) AsMap() pcommon.Map {
	return pcommon.Map(m)
}

// IsZero whether Attrs is zero value.
func (m Attrs) IsZero() bool {
	return m == (Attrs{})
}

// CopyTo copies all attributes from m to target.
func (m Attrs) CopyTo(target pcommon.Map) {
	m.AsMap().CopyTo(target)
}
