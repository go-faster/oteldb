// Package oteldbexporter contains oteldb exporter factory.
package oteldbexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

const (
	typeStr   = "oteldbexporter"
	stability = component.StabilityLevelDevelopment
)

// NewFactory creates new factory of [Exporter].
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, stability),
		exporter.WithMetrics(createMetricsExporter, stability),
		exporter.WithLogs(createLogsExporter, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		DSN: "clickhouse://localhost:9000",
	}
}

func createTracesExporter(
	ctx context.Context,
	settings exporter.CreateSettings,
	cfg component.Config,
) (exporter.Traces, error) {
	ecfg := cfg.(*Config)
	inserter, err := ecfg.connect(ctx, settings)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTracesExporter(ctx, settings, cfg, tracestorage.NewConsumer(inserter).ConsumeTraces)
}

func createMetricsExporter(
	ctx context.Context,
	settings exporter.CreateSettings,
	cfg component.Config,
) (exporter.Metrics, error) {
	ecfg := cfg.(*Config)
	inserter, err := ecfg.connect(ctx, settings)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetricsExporter(ctx, settings, cfg, inserter.ConsumeMetrics)
}

func createLogsExporter(
	ctx context.Context,
	settings exporter.CreateSettings,
	cfg component.Config,
) (exporter.Logs, error) {
	ecfg := cfg.(*Config)
	inserter, err := ecfg.connect(ctx, settings)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogsExporter(ctx, settings, cfg, logstorage.NewConsumer(inserter).ConsumeLogs)
}
