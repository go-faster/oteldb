// Package iterators define storage iterator interfaces and some utilities.
package iterators

// Iterator is an storage iterator interface.
type Iterator[T any] interface {
	// Next returns true, if there is element and fills t.
	Next(t *T) bool
	// Err returns an error caused during iteration, if any.
	Err() error
	// Close closes iterator.
	Close() error
}

// ForEach calls given callback for each iterator element.
//
// NOTE: ForEach does not close iterator.
func ForEach[T any](i Iterator[T], cb func(T) error) error {
	var t T
	for i.Next(&t) {
		if err := cb(t); err != nil {
			return err
		}
	}
	return i.Err()
}

var _ Iterator[any] = (*EmptyIterator[any])(nil)

// EmptyIterator returns zero elements.
type EmptyIterator[T any] struct{}

// Empty creates new empty iterator
func Empty[T any]() *EmptyIterator[T] {
	return &EmptyIterator[T]{}
}

// Next returns true, if there is element and fills t.
func (i *EmptyIterator[T]) Next(*T) bool {
	return false
}

// Err returns an error caused during iteration, if any.
func (i *EmptyIterator[T]) Err() error {
	return nil
}

// Close closes iterator.
func (i *EmptyIterator[T]) Close() error {
	return nil
}

var _ Iterator[any] = (*SliceIterator[any])(nil)

// SliceIterator is a slice iterator.
type SliceIterator[T any] struct {
	data []T
	n    int
}

// Slice creates new SliceIterator from given values.
func Slice[T any](vals []T) *SliceIterator[T] {
	return &SliceIterator[T]{
		data: vals,
		n:    0,
	}
}

// Next returns true, if there is element and fills t.
func (i *SliceIterator[T]) Next(t *T) bool {
	if i.n >= len(i.data) {
		return false
	}
	*t = i.data[i.n]
	i.n++
	return true
}

// Err returns an error caused during iteration, if any.
func (i *SliceIterator[T]) Err() error {
	return nil
}

// Close closes iterator.
func (i *SliceIterator[T]) Close() error {
	return nil
}
