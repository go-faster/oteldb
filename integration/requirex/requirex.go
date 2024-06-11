// Package requirex provides additional testing helpers.
package requirex

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/stretchr/testify/require"
)

// Unique checks that given slice is unique.
func Unique[S ~[]E, E comparable](t require.TestingT, s S) {
	m := make(map[E]struct{}, len(s))
	for _, v := range s {
		if _, ok := m[v]; ok {
			require.Fail(t, fmt.Sprintf("slice %#v is not unique (duplicated entry: %#v)", s, v))
			return
		}
		m[v] = struct{}{}
	}
}

// Sorted checks that given slice is sorted.
func Sorted[S ~[]E, E cmp.Ordered](t require.TestingT, s S) {
	if !slices.IsSorted(s) {
		require.Fail(t, fmt.Sprintf("slice %#v is not sorted", s))
	}
}
