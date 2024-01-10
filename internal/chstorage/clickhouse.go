package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
)

type ClickhouseClient interface {
	Do(ctx context.Context, q ch.Query) error
	Ping(ctx context.Context) error
}

type dialingClickhouseClient struct {
	options ch.Options
}

func (c *dialingClickhouseClient) Ping(ctx context.Context) error {
	db, err := ch.Dial(ctx, c.options)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	return db.Ping(ctx)
}

var _ ClickhouseClient = (*dialingClickhouseClient)(nil)

func NewDialingClickhouseClient(options ch.Options) ClickhouseClient {
	return &dialingClickhouseClient{
		options: options,
	}
}

func (c *dialingClickhouseClient) Do(ctx context.Context, q ch.Query) error {
	db, err := ch.Dial(ctx, c.options)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	return db.Do(ctx, q)
}
