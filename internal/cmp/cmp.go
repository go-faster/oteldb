// Package cmp is a simple utility package for comparing values.
package cmp

import "golang.org/x/exp/constraints"

// Compare is a comparator.
func Compare[T constraints.Ordered](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
