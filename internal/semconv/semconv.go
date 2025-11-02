package semconv

import (
	"go.opentelemetry.io/otel/attribute"
)

type SignalType string

const (
	SignalMetrics SignalType = "metrics"
	SignalTraces  SignalType = "traces"
	SignalLogs    SignalType = "logs"
)

func Signal(s SignalType) attribute.KeyValue {
	return attribute.String("chstorage.signal", string(s))
}
