package logqlmetric

import (
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// FPoint is a metric point.
type FPoint struct {
	Timestamp otelstorage.Timestamp
	Value     float64
}

// MilliT returns Prometheus millisecond timestamp.
func (p FPoint) MilliT() int64 {
	return p.Timestamp.AsTime().UnixMilli()
}

// Sample is a metric sample extracted from logs.
type Sample struct {
	Data float64
	Set  AggregatedLabels
}

// Series is a grouped set of metric points.
type Series struct {
	Data []FPoint
	Set  AggregatedLabels
}

// SampledEntry is a sampled log entry.
type SampledEntry struct {
	Sample    float64
	Timestamp otelstorage.Timestamp
	Set       AggregatedLabels
}
