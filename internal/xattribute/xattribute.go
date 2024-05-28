// Package xattribute provides some helpers to create OpenTelemetry attributes.
package xattribute

import (
	"fmt"
	"slices"

	"go.opentelemetry.io/otel/attribute"
)

// StringerSlice creates a string slice attribute from slice of [fmt.Stringer] implementations.
func StringerSlice[S ~[]E, E fmt.Stringer](k string, v S) attribute.KeyValue {
	if len(v) == 0 {
		return attribute.StringSlice(k, nil)
	}

	// Using pooled string slice is fine, since attribute package copies slice.
	ss := getStringSlice()
	defer putStringSlice(ss)

	ss.val = append(ss.val, make([]string, len(v))...)
	for i, f := range v {
		ss.val[i] = safeStringer(f)
	}
	return attribute.StringSlice(k, ss.val)
}

func safeStringer[F fmt.Stringer](f F) (v string) {
	defer func() {
		if r := recover(); r != nil {
			v = "<stringer panic>:" + fmt.Sprint(r)
		}
	}()
	return f.String()
}

// StringMap creates a sorted string slice attribute from string map.
func StringMap(k string, m map[string]string) attribute.KeyValue {
	if len(m) == 0 {
		return attribute.StringSlice(k, nil)
	}

	// Using pooled string slice is fine, since attribute package copies slice.
	ss := getStringSlice()
	defer putStringSlice(ss)

	for k, v := range m {
		ss.val = append(ss.val, k+"="+v)
	}
	slices.Sort(ss.val)

	return attribute.StringSlice(k, ss.val)
}
