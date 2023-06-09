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
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
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

// Receiver is a OpenTelemetry-compatible trace receiver.
type Receiver struct {
	receivers []component.Component

	fatal     chan struct{}
	fatalOnce sync.Once
	logger    *zap.Logger
}

var defaultReceivers = map[string]any{
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
func NewReceiver(consumers Consumers, cfg ReceiverConfig) (*Receiver, error) {
	cfg.setDefaults()
	shim := &Receiver{
		fatal:  make(chan struct{}),
		logger: cfg.Logger.Named("shim"),
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

	var (
		tracesConsumer  consumer.Traces
		metricsConsumer consumer.Metrics
		logsConsumer    consumer.Logs

		pipelines = map[string]any{}
	)
	if impl := consumers.Traces; impl != nil {
		tracesConsumer, err = consumer.NewTraces(func(ctx context.Context, ld ptrace.Traces) error {
			if err := impl.ConsumeTraces(ctx, ld); err != nil {
				shim.logger.Error("Consume traces", zap.Error(err))
			}
			return nil
		})
		if err != nil {
			return nil, errors.Wrap(err, "create traces consumer")
		}

		pipelines["traces"] = map[string]any{
			"exporters": []string{"nop"}, // nop exporter to avoid errors
			"receivers": receivers,
		}
	}
	if impl := consumers.Metrics; impl != nil {
		metricsConsumer, err = consumer.NewMetrics(func(ctx context.Context, ld pmetric.Metrics) error {
			if err := impl.ConsumeMetrics(ctx, ld); err != nil {
				shim.logger.Error("Consume metrics", zap.Error(err))
			}
			return nil
		})
		if err != nil {
			return nil, errors.Wrap(err, "create metrics consumer")
		}

		pipelines["metrics"] = map[string]any{
			"exporters": []string{"nop"}, // nop exporter to avoid errors
			"receivers": receivers,
		}
	}
	if impl := consumers.Logs; impl != nil {
		logsConsumer, err = consumer.NewLogs(func(ctx context.Context, ld plog.Logs) error {
			if err := impl.ConsumeLogs(ctx, ld); err != nil {
				shim.logger.Error("Consume logs", zap.Error(err))
			}
			return nil
		})
		if err != nil {
			return nil, errors.Wrap(err, "create logs consumer")
		}

		pipelines["logs"] = map[string]any{
			"exporters": []string{"nop"}, // nop exporter to avoid errors
			"receivers": receivers,
		}
	}
	if len(pipelines) == 0 {
		return nil, errors.New("at least one consumer must be set")
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
					"pipelines": pipelines,
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

		if c := tracesConsumer; c != nil {
			params := receiver.CreateSettings{
				TelemetrySettings: component.TelemetrySettings{
					Logger:         logger.Named("traces"),
					TracerProvider: cfg.TracerProvider,
					MeterProvider:  cfg.MeterProvider,
				},
			}

			recv, err := factoryBase.CreateTracesReceiver(ctx, params, componentCfg, c)
			if err != nil {
				if errors.Is(err, component.ErrDataTypeIsNotSupported) {
					continue
				}
				return nil, errors.Wrap(err, "create traces receiver")
			}

			shim.receivers = append(shim.receivers, recv)
		}
		if c := metricsConsumer; c != nil {
			params := receiver.CreateSettings{
				TelemetrySettings: component.TelemetrySettings{
					Logger:         logger.Named("metrics"),
					TracerProvider: cfg.TracerProvider,
					MeterProvider:  cfg.MeterProvider,
				},
			}

			recv, err := factoryBase.CreateMetricsReceiver(ctx, params, componentCfg, c)
			if err != nil {
				if errors.Is(err, component.ErrDataTypeIsNotSupported) {
					continue
				}
				return nil, errors.Wrap(err, "create metrics receiver")
			}

			shim.receivers = append(shim.receivers, recv)
		}
		if c := logsConsumer; c != nil {
			params := receiver.CreateSettings{
				TelemetrySettings: component.TelemetrySettings{
					Logger:         logger.Named("logs"),
					TracerProvider: cfg.TracerProvider,
					MeterProvider:  cfg.MeterProvider,
				},
			}

			recv, err := factoryBase.CreateLogsReceiver(ctx, params, componentCfg, c)
			if err != nil {
				if errors.Is(err, component.ErrDataTypeIsNotSupported) {
					continue
				}
				return nil, errors.Wrap(err, "create logs receiver")
			}

			shim.receivers = append(shim.receivers, recv)
		}
	}

	return shim, nil
}

// Run setups corresponding listeners.
func (r *Receiver) Run(ctx context.Context) (rerr error) {
	var running []component.Component
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

func shutdown(receivers []component.Component) error {
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

// ReportFatalError implements component.Host
func (r *Receiver) ReportFatalError(err error) {
	r.logger.Error("Fatal receiver error", zap.Error(err))
	r.fatalOnce.Do(func() {
		close(r.fatal)
	})
}

// GetFactory implements component.Host
func (r *Receiver) GetFactory(component.Kind, component.Type) component.Factory {
	return nil
}

// GetExtensions implements component.Host
func (r *Receiver) GetExtensions() map[component.ID]extension.Extension { return nil }

// GetExporters implements component.Host
func (r *Receiver) GetExporters() map[component.DataType]map[component.ID]component.Component {
	return nil
}
