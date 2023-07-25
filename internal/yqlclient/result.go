package yqlclient

import (
	"context"
	"io"

	"github.com/go-faster/errors"
	"go.ytsaurus.tech/yt/go/yson"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/yqlclient/ytqueryapi"
)

// ReadResult reads result of given query ID.
func ReadResult[T any](ctx context.Context, c *Client, queryID ytqueryapi.QueryID, format ResultFormat[T]) (iterators.Iterator[T], error) {
	data, err := c.client.ReadQueryResult(ctx, ytqueryapi.ReadQueryResultParams{
		QueryID:      queryID,
		OutputFormat: format.Format(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "read query result")
	}

	return format.Iter(data)
}

// ResultFormat defines result decoder.
type ResultFormat[T any] interface {
	Format() ytqueryapi.OutputFormat
	Iter(r io.Reader) (iterators.Iterator[T], error)
}

// YSONFormat decodes result as YSON.
type YSONFormat[T any] struct{}

// Format implements [ResultFormat].
func (YSONFormat[T]) Format() ytqueryapi.OutputFormat {
	return ytqueryapi.OutputFormatYson
}

// Iter implements [ResultFormat].
func (YSONFormat[T]) Iter(r io.Reader) (iterators.Iterator[T], error) {
	return &ysonIterator[T]{reader: yson.NewReaderKind(r, yson.StreamListFragment)}, nil
}

type ysonIterator[T any] struct {
	reader *yson.Reader
	err    error
}

func (i *ysonIterator[T]) Next(to *T) bool {
	ok, err := i.reader.NextListItem()
	if !ok || err != nil {
		i.err = err
		return false
	}

	raw, err := i.reader.NextRawValue()
	if err != nil {
		i.err = errors.Wrap(err, "read next value")
		return false
	}

	if err := yson.Unmarshal(raw, to); err != nil {
		i.err = errors.Wrap(err, "unmarshal")
		return false
	}

	return true
}

func (i *ysonIterator[T]) Err() error {
	return i.err
}

func (i *ysonIterator[T]) Close() error {
	return nil
}
