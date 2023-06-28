// Package logstorage defines storage structure for logs storage.
package logstorage

import (
	"context"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Querier is a logs storage query interface.
type Querier interface {
	// LabelNames returns all available label names.
	LabelNames(ctx context.Context, opts LabelsOptions) ([]string, error)
	// LabelValues returns all available label values for given label.
	LabelValues(ctx context.Context, labelName string, opts LabelsOptions) (iterators.Iterator[Label], error)
}

// LabelsOptions defines options for Labels and LabelValues methods.
type LabelsOptions struct {
	// Start defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	Start otelstorage.Timestamp
	// End defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	End otelstorage.Timestamp
}

// Inserter is a log storage insert interface.
type Inserter interface {
	// InsertRecords inserts given records.
	InsertRecords(ctx context.Context, records []Record) error
	// InsertLogLabels insert given set of labels to the storage.
	InsertLogLabels(ctx context.Context, labels map[Label]struct{}) error
}
