// Package otelreceiver provides simple wrapper to setup trace receiver.
package otelreceiver

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/go-faster/errors"

	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Consumer is a trace consumer.
type Consumer interface {
	ConsumeTraces(ctx context.Context, td ptrace.Traces) error
}

// Receiver is a OpenTelemetry-compatible trace receiver.
type Receiver[C Consumer] struct {
	consumer  C
	receivers []receiver.Traces

	fatal     chan struct{}
	fatalOnce sync.Once
	logger    *zap.Logger
}

var defaultReceivers = map[string]any{
	// },
	"otlp": map[string]any{
		"protocols": map[string]any{
			"grpc": nil,
			"http": nil,
		},
	},
}

// ReceiverConfig is a config struct for Receiver.
type ReceiverConfig struct {
	OTELConfig     map[string]any
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Logger         *zap.Logger
}

func (cfg *ReceiverConfig) setDefaults() {
	if cfg.OTELConfig == nil {
		cfg.OTELConfig = defaultReceivers
	}
	if cfg.TracerProvider == nil {
		cfg.TracerProvider = otel.GetTracerProvider()
	}
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
}

// NewReceiver setups trace receiver.
func NewReceiver[C Consumer](c C, cfg ReceiverConfig) (*Receiver[C], error) {
	cfg.setDefaults()
	shim := &Receiver[C]{
		consumer: c,
		fatal:    make(chan struct{}),
		logger:   cfg.Logger.Named("shim"),
	}

	receiverFactories, err := receiver.MakeFactoryMap(otlpreceiver.NewFactory())
	if err != nil {
		return nil, err
	}

	receiverCfg := cfg.OTELConfig
	receivers := make([]string, 0, len(receiverCfg))
	for k := range receiverCfg {
		receivers = append(receivers, k)
	}

	// Creates a config provider with the given config map.
	// The provider will be used to retrieve the actual config for the pipeline (although we only need the receivers).
	pro, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{
		ResolverSettings: confmap.ResolverSettings{
			URIs: []string{"mock:/"},
			Providers: map[string]confmap.Provider{"mock": &mapProvider{raw: map[string]any{
				"receivers": receiverCfg,
				"exporters": map[string]any{
					"nop": map[string]any{},
				},
				"service": map[string]any{
					"pipelines": map[string]any{
						"traces": map[string]any{
							"exporters": []string{"nop"}, // nop exporter to avoid errors
							"receivers": receivers,
						},
					},
				},
			}}},
		},
	})
	if err != nil {
		return nil, err
	}

	// Creates the configuration for the pipeline.
	// We only need the receivers, the rest of the configuration is not used.
	conf, err := pro.Get(context.Background(), otelcol.Factories{
		Receivers: receiverFactories,
		Exporters: map[component.Type]exporter.Factory{"nop": exportertest.NewNopFactory()}, // nop exporter to avoid errors
	})
	if err != nil {
		return nil, err
	}

	traceConsumer, err := consumer.NewTraces(shim.consumer.ConsumeTraces)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	for componentID, componentCfg := range conf.Receivers {
		factoryBase := receiverFactories[componentID.Type()]
		if factoryBase == nil {
			return nil, errors.Errorf("receiver factory not found for type: %s", componentID.Type())
		}

		// Make sure that the headers are added to context. Required for Authentication.
		if componentID.Type() == "otlp" {
			otlpRecvCfg := componentCfg.(*otlpreceiver.Config)

			if otlpRecvCfg.HTTP != nil {
				otlpRecvCfg.HTTP.IncludeMetadata = true
				componentCfg = otlpRecvCfg
			}
		}

		logger := cfg.Logger
		if name := string(componentID.Type()); name != "" {
			logger = logger.Named(name)
		}
		params := receiver.CreateSettings{
			TelemetrySettings: component.TelemetrySettings{
				Logger:         logger,
				TracerProvider: cfg.TracerProvider,
				MeterProvider:  cfg.MeterProvider,
			},
		}

		recv, err := factoryBase.CreateTracesReceiver(ctx, params, componentCfg, traceConsumer)
		if err != nil {
			return nil, err
		}

		shim.receivers = append(shim.receivers, recv)
	}

	return shim, nil
}

// Run setups corresponding listeners.
func (r *Receiver[H]) Run(ctx context.Context) (rerr error) {
	var running []receiver.Traces
	defer func() {
		multierr.AppendInto(&rerr, shutdown(running))
	}()

	for _, recv := range r.receivers {
		if err := recv.Start(ctx, r); err != nil {
			return err
		}
		running = append(running, recv)
	}

	select {
	case <-ctx.Done():
	case <-r.fatal:
	}

	return nil
}

func shutdown(receivers []receiver.Traces) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var errs []error
	for _, r := range receivers {
		if err := r.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, err)
		}
	}

	return multierr.Combine(errs...)
}

func (r *Receiver[H]) consume(ctx context.Context, traces ptrace.Traces) error {
	// TODO(tdakkota): instrument?
	if err := r.consumer.ConsumeTraces(ctx, traces); err != nil {
		r.logger.Error("Consume failed", zap.Error(err))
	}
	return nil
}

// ReportFatalError implements component.Host
func (r *Receiver[H]) ReportFatalError(err error) {
	r.logger.Error("Fatal receiver error", zap.Error(err))
	r.fatalOnce.Do(func() {
		close(r.fatal)
	})
}

// GetFactory implements component.Host
func (r *Receiver[H]) GetFactory(component.Kind, component.Type) component.Factory {
	return nil
}

// GetExtensions implements component.Host
func (r *Receiver[H]) GetExtensions() map[component.ID]extension.Extension { return nil }

// GetExporters implements component.Host
func (r *Receiver[H]) GetExporters() map[component.DataType]map[component.ID]component.Component {
	return nil
}
