// Package metricstorage defines some interfaces for metric storage.
package metricstorage

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// OptimizedSeriesQuerier defines API for optimal series querying.
type OptimizedSeriesQuerier interface {
	OnlySeries(ctx context.Context, sortSeries bool, startMs, endMs int64, matcherSets ...[]*labels.Matcher) storage.SeriesSet
}
