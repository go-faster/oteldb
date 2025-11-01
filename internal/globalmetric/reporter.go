package globalmetric

import (
	"context"

	"github.com/ClickHouse/ch-go"
)

type ClickHouseClient interface {
	Do(ctx context.Context, q ch.Query) error
}
type Reporter struct {
	client ClickHouseClient
}
