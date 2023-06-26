// Package logstorage defines storage structure for logs storage.
package logstorage

import (
	"context"
)

// Inserter is a log storage insert interface.
type Inserter interface {
	// InsertRecords inserts given records.
	InsertRecords(ctx context.Context, records []Record) error
	// InsertLogLabels insert given set of labels to the storage.
	InsertLogLabels(ctx context.Context, labels map[Label]struct{}) error
}
