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
func ReadResult[T any](ctx context.Context, c *Client, queryID ytqueryapi.QueryID) (iterators.Iterator[T], error) {
	data, err := c.client.ReadQueryResult(ctx, ytqueryapi.ReadQueryResultParams{
		QueryID:      queryID,
		OutputFormat: ytqueryapi.OutputFormatYson,
	})
	if err != nil {
		return nil, errors.Wrap(err, "read query result")
	}

	return &resultIterator[T]{dec: yson.NewDecoder(data)}, nil
}

type resultIterator[T any] struct {
	dec *yson.Decoder
	err error
}

func (i *resultIterator[T]) Next(t *T) bool {
	switch err := i.dec.Decode(t); err {
	case io.EOF:
		return false
	case nil:
		return true
	default:
		i.err = err
		return false
	}
}

func (i *resultIterator[T]) Err() error {
	return i.err
}

func (i *resultIterator[T]) Close() error {
	return nil
}
