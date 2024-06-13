// Package logstorage defines storage structure for logs storage.
package logstorage

import (
	"context"
	"time"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
)

// Querier is a logs storage query interface.
type Querier interface {
	// LabelNames returns all available label names.
	LabelNames(ctx context.Context, opts LabelsOptions) ([]string, error)
	// LabelValues returns all available label values for given label.
	LabelValues(ctx context.Context, labelName string, opts LabelsOptions) (iterators.Iterator[Label], error)
	// Series returns all available log series.
	Series(ctx context.Context, opts SeriesOptions) (Series, error)
}

// LabelsOptions defines options for [Querier.LabelNames] and [Querier.LabelValues] methods.
type LabelsOptions struct {
	// Start defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	Start time.Time
	// End defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	End time.Time
	// Selector that selects the streams to match.
	Query logql.Selector
}

// SeriesOptions defines options for [Querier.Series] method.
type SeriesOptions struct {
	// Start defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	Start time.Time
	// End defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	End time.Time
	// Selectors defines a list of matchers to filter series.
	Selectors []logql.Selector
}

// Inserter is a log storage insert interface.
type Inserter interface {
	// InsertRecords inserts given records.
	InsertRecords(ctx context.Context, records []Record) error
	// InsertLogLabels insert given set of labels to the storage.
	InsertLogLabels(ctx context.Context, labels map[Label]struct{}) error
}
