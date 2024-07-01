package logqlengine

import (
	"go.opentelemetry.io/otel/metric"
)

type engineStats struct {
	QueryDuration metric.Float64Histogram
}

// Register creates metrics using given [metric.Meter].
func (e *engineStats) Register(meter metric.Meter) error {
	var err error
	e.QueryDuration, err = meter.Float64Histogram("logql.query_duration",
		metric.WithUnit("s"),
		metric.WithDescription("LogQL request duration in seconds"),
	)
	if err != nil {
		return err
	}
	return nil
}
