// Package prometheusremotewrite contains translator from Prometheus remote write format to OTLP metrics.
package prometheusremotewrite

import "go.uber.org/zap"

// Settings defines translation settings.
type Settings struct {
	Namespace         string
	ExternalLabels    map[string]string
	DisableTargetInfo bool
	AddMetricSuffixes bool
	TimeThreshold     int64
	Logger            zap.Logger
}
