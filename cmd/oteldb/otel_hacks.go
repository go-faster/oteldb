package main

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Metrics wraps TracerProvider and MeterProvider.
type Metrics interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
	TextMapPropagator() propagation.TextMapPropagator
}

// MetricsOverride implements Metrics overrider.
type MetricsOverride struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	propagator     propagation.TextMapPropagator
}

// NewMetricsOverride initializes a new MetricsOverride from parent Metrics.
func NewMetricsOverride(m Metrics) *MetricsOverride {
	return &MetricsOverride{
		tracerProvider: m.TracerProvider(),
		meterProvider:  m.MeterProvider(),
		propagator:     m.TextMapPropagator(),
	}
}

// TextMapPropagator returns the propagation.TextMapPropagator.
func (m *MetricsOverride) TextMapPropagator() propagation.TextMapPropagator {
	return m.propagator
}

// TracerProvider returns the trace.TracerProvider.
func (m *MetricsOverride) TracerProvider() trace.TracerProvider {
	return m.tracerProvider
}

// MeterProvider returns the metric.MeterProvider.
func (m *MetricsOverride) MeterProvider() metric.MeterProvider {
	return m.meterProvider
}

// WithTracerProvider overrides the trace.TracerProvider.
func (m *MetricsOverride) WithTracerProvider(tracerProvider trace.TracerProvider) *MetricsOverride {
	m.tracerProvider = tracerProvider
	return m
}

// WithMeterProvider overrides the metric.MeterProvider.
func (m *MetricsOverride) WithMeterProvider(meterProvider metric.MeterProvider) *MetricsOverride {
	m.meterProvider = meterProvider
	return m
}
