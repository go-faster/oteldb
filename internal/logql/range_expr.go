package logql

import "time"

// LogRangeExpr is a log range aggregation expression.
//
// See https://grafana.com/docs/loki/latest/logql/metric_queries/#log-range-aggregations.
type LogRangeExpr struct {
	Sel      Selector
	Range    time.Duration
	Pipeline []PipelineStage
	Unwrap   *UnwrapExpr
	Offset   *OffsetExpr
}

// UnwrapExpr sets labels to perform aggregation.
//
// See https://grafana.com/docs/loki/latest/logql/metric_queries/#unwrapped-range-aggregations.
type UnwrapExpr struct {
	Op      string
	Label   Label
	Filters []LabelMatcher
}

// OffsetExpr defines aggregation time offset.
type OffsetExpr struct {
	Duration time.Duration
}
