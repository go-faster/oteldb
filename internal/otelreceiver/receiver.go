// Package otelreceiver provides simple wrapper to setup trace receiver.
package otelreceiver

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"

	"github.com/go-faster/oteldb/internal/otelreceiver/oteldbexporter"
	"github.com/go-faster/oteldb/internal/otelreceiver/prometheusremotewritereceiver"
)

func receiverFactoryMap() (map[component.Type]receiver.Factory, error) {
	return receiver.MakeFactoryMap(
		otlpreceiver.NewFactory(),
		prometheusremotewritereceiver.NewFactory(),
	)
}

func processorFactoryMap() (map[component.Type]processor.Factory, error) {
	return processor.MakeFactoryMap(
		batchprocessor.NewFactory(),
	)
}

func exporterFactoryMap() (map[component.Type]exporter.Factory, error) {
	return exporter.MakeFactoryMap(
		oteldbexporter.NewFactory(),
	)
}

// Factories returns oteldb factories list.
func Factories() (f otelcol.Factories, _ error) {
	receivers, err := receiverFactoryMap()
	if err != nil {
		return f, errors.Wrap(err, "get receiver factory map")
	}

	processors, err := processorFactoryMap()
	if err != nil {
		return f, errors.Wrap(err, "get processor factory map")
	}

	exporters, err := exporterFactoryMap()
	if err != nil {
		return f, errors.Wrap(err, "get exporter factory map")
	}

	return otelcol.Factories{
		Receivers:  receivers,
		Processors: processors,
		Exporters:  exporters,
	}, nil
}
