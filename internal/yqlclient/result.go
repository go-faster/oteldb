package yqlclient

import (
	"context"

	"github.com/go-faster/errors"
	"go.ytsaurus.tech/yt/go/yson"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/yqlclient/ytqueryapi"
)

// ReadResult reads result of given query ID.
func ReadResult[T any](ctx context.Context, c *Client, queryID ytqueryapi.QueryID) (iterators.Iterator[T], error) {
	data, err := c.client.ReadQueryResult(ctx, ytqueryapi.ReadQueryResultParams{
		QueryID:      queryID,
		OutputFormat: ytqueryapi.OutputFormatYson,
	})
	if err != nil {
		return nil, errors.Wrap(err, "read query result")
	}

	return &resultIterator[T]{reader: yson.NewReaderKind(data, yson.StreamListFragment)}, nil
}

type resultIterator[T any] struct {
	reader *yson.Reader
	err    error
}

func (i *resultIterator[T]) Next(to *T) bool {
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

func (i *resultIterator[T]) Err() error {
	return i.err
}

func (i *resultIterator[T]) Close() error {
	return nil
}
