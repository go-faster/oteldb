package main

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/opentracing/opentracing-go"
	"go.opentelemetry.io/otel"
	otelBridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/autozpages"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, metrics *app.Metrics) error {
		shutdown, err := autozpages.Setup(metrics.TracerProvider())
		if err != nil {
			return errors.Wrap(err, "setup zPages")
		}
		defer func() {
			_ = shutdown(context.Background())
		}()

		m := NewMetricsOverride(metrics)
		{
			// Setting OpenTelemetry/OpenTracing Bridge.
			// https://github.com/open-telemetry/opentelemetry-go/tree/main/bridge/opentracing#opentelemetryopentracing-bridge
			otelTracer := metrics.TracerProvider().Tracer("yt")
			bridgeTracer, wrapperTracerProvider := otelBridge.NewTracerPair(otelTracer)
			opentracing.SetGlobalTracer(bridgeTracer)

			// Override for context propagation.
			otel.SetTracerProvider(wrapperTracerProvider)
			m = m.WithTracerProvider(wrapperTracerProvider)
		}

		root, err := newApp(ctx, lg, m)
		if err != nil {
			return errors.Wrap(err, "setup")
		}
		return root.Run(ctx)
	})
}
