package logqlengine

import (
	"context"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Querier does queries to storage.
type Querier interface {
	SelectLogs(ctx context.Context, start, end otelstorage.Timestamp, params SelectLogsParams) (iterators.Iterator[logstorage.Record], error)
}

// SelectLogsParams is a storage query params.
type SelectLogsParams struct {
	Labels []logql.LabelMatcher
}
