package logqlengine

import "golang.org/x/exp/constraints"

// Comparator is a filter that compares value.
type Comparator[T any] interface {
	Compare(a, b T) bool
}

// EqComparator implements '==' Comparator.
type EqComparator[T comparable] struct{}

// Compare implements Comparator[T].
func (EqComparator[T]) Compare(a, b T) bool {
	return a == b
}

// NotEqComparator implements '!=' Comparator.
type NotEqComparator[T comparable] struct{}

// Compare implements Comparator[T].
func (NotEqComparator[T]) Compare(a, b T) bool {
	return a != b
}

// LtComparator implements '<' Comparator.
type LtComparator[T constraints.Ordered] struct{}

// Compare implements Comparator[T].
func (LtComparator[T]) Compare(a, b T) bool {
	return a < b
}

// LteComparator implements '<=' Comparator.
type LteComparator[T constraints.Ordered] struct{}

// Compare implements Comparator[T].
func (LteComparator[T]) Compare(a, b T) bool {
	return a <= b
}

// GtComparator implements  '>' Comparator.
type GtComparator[T constraints.Ordered] struct{}

// Compare implements Comparator[T].
func (GtComparator[T]) Compare(a, b T) bool {
	return a > b
}

// GteComparator implements '>=' Comparator.
type GteComparator[T constraints.Ordered] struct{}

// Compare implements Comparator[T].
func (GteComparator[T]) Compare(a, b T) bool {
	return a >= b
}
