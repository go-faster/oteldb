package ytstorage

import (
	"context"

	"go.ytsaurus.tech/yt/go/yt"

	"github.com/go-faster/oteldb/internal/iterators"
)

// YTQLQuerier implements tracestorage.Querier based on YTSaurus QL.
type YTQLQuerier struct {
	yc     yt.TabletClient
	tables Tables
}

// NewYTQLQuerier creates new YTQLQuerier.
func NewYTQLQuerier(yc yt.TabletClient, tables Tables) *YTQLQuerier {
	return &YTQLQuerier{
		yc:     yc,
		tables: tables,
	}
}

var _ iterators.Iterator[any] = (*ytIterator[any])(nil)

type ytIterator[T any] struct {
	reader yt.TableReader
	err    error
}

// Next returns true, if there is element and fills t.
func (i *ytIterator[T]) Next(t *T) bool {
	if i.err != nil {
		return false
	}

	ok := i.reader.Next()
	if !ok {
		return false
	}

	i.err = i.reader.Scan(t)
	return i.err == nil
}

// Err returns an error caused during iteration, if any.
func (i *ytIterator[T]) Err() error {
	if e := i.err; e != nil {
		return e
	}
	return i.reader.Err()
}

// Close closes iterator.
func (i *ytIterator[T]) Close() error {
	return i.reader.Close()
}

func queryRows[T any](ctx context.Context, yc yt.TabletClient, q string, cb func(T)) error {
	r, err := yc.SelectRows(ctx, q, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = r.Close()
	}()
	for r.Next() {
		var val T
		if err := r.Scan(&val); err != nil {
			return err
		}
		cb(val)
	}
	return r.Err()
}
