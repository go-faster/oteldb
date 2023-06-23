package otelreceiver

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TracesConsumer is traces consumer.
type TracesConsumer interface {
	ConsumeTraces(ctx context.Context, td ptrace.Traces) error
}

// MetricsConsumer is metrics consumer.
type MetricsConsumer interface {
	ConsumeMetrics(ctx context.Context, ld pmetric.Metrics) error
}

// LogsConsumer is logs consumer.
type LogsConsumer interface {
	ConsumeLogs(ctx context.Context, ld plog.Logs) error
}

// Consumers is a set of telemetry consumers.
type Consumers struct {
	Traces  TracesConsumer
	Metrics MetricsConsumer
	Logs    LogsConsumer
}
