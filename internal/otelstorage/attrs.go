package otelstorage

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// KeyToLabel converts key to label name.
func KeyToLabel(key string) string {
	isDigit := func(r byte) bool {
		return r >= '0' && r <= '9'
	}
	isAlpha := func(r byte) bool {
		return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
	}

	var label strings.Builder
	for i, r := range []byte(key) {
		switch {
		case isDigit(r):
			// Label could not start with digit.
			if i == 0 {
				label.Grow(len(key) + 1)
				label.WriteString("_")
				goto slow
			}
		case r >= 0x80 && r == '_' || isAlpha(r):
		default:
			label.Grow(len(key))
			label.WriteString(key[:i])
			key = key[i:]
			goto slow
		}
	}
	return key
slow:
	for _, r := range []byte(key) {
		if r >= 0x80 || r == '_' || isDigit(r) || isAlpha(r) {
			label.WriteByte(r)
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

// Hash computes attributes [Hash].
func (m Attrs) Hash() Hash {
	return AttrHash(m.AsMap())
}

// IsZero whether Attrs is zero value.
func (m Attrs) IsZero() bool {
	return m == (Attrs{})
}

// Len returns number of attributes.
func (m Attrs) Len() int {
	if m.IsZero() {
		return 0
	}
	return m.AsMap().Len()
}

// CopyTo copies all attributes from m to target.
func (m Attrs) CopyTo(target pcommon.Map) {
	m.AsMap().CopyTo(target)
}
