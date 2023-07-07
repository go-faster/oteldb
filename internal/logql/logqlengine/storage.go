package logqlengine

import (
	"context"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// SupportedOps is a bitset defining ops supported by Querier.
type SupportedOps uint64

// Add sets capability.
func (caps *SupportedOps) Add(ops ...logql.BinOp) {
	for _, op := range ops {
		*caps |= SupportedOps(1 << op)
	}
}

// Supports checks if storage supports given ops.
func (caps SupportedOps) Supports(op logql.BinOp) bool {
	mask := SupportedOps(1 << op)
	return caps&mask != 0
}

// QuerierСapabilities defines what operations storage can do.
type QuerierСapabilities struct {
	Label SupportedOps
}

// Querier does queries to storage.
type Querier interface {
	// Сapabilities returns Querier capabilities.
	//
	// NOTE: engine would call once and then save value.
	// 	Сapabilities should not change over time.
	Сapabilities() QuerierСapabilities
	// SelectLogs selects log records from storage.
	SelectLogs(ctx context.Context, start, end otelstorage.Timestamp, params SelectLogsParams) (iterators.Iterator[logstorage.Record], error)
}

// SelectLogsParams is a storage query params.
type SelectLogsParams struct {
	Labels []logql.LabelMatcher
}
