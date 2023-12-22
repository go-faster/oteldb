// Package prometheusremotewrite contains translator from Prometheus remote write format to OTLP metrics.
package prometheusremotewrite

import "go.uber.org/zap"

// Settings defines translation settings.
type Settings struct {
	TimeThreshold int64
	Logger        zap.Logger
}
