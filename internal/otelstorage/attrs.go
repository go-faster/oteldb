package otelstorage

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// KeyToLabel converts key to label name.
func KeyToLabel(key string) string {
	isDigit := func(r rune) bool {
		return r >= '0' && r <= '9'
	}
	isAlpha := func(r rune) bool {
		return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
	}

	var label strings.Builder
	for i, r := range key {
		switch {
		case isDigit(r):
			// Label could not start with digit.
			if i == 0 {
				label.WriteString("_")
				goto slow
			}
		case r == '_' || isAlpha(r):
		default:
			label.WriteString(key[:i])
			key = key[i:]
			goto slow
		}
	}
	return key
slow:
	for _, r := range key {
		if r == '_' || isDigit(r) || isAlpha(r) {
			label.WriteRune(r)
			continue
		}
		// Replace rune with '_'.
		label.WriteByte('_')
	}
	return label.String()
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
