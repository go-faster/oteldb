package main

import (
	"context"
	"os"
	"strings"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/go-faster/oteldb/internal/autozpages"
	"github.com/go-faster/oteldb/internal/zapotel"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		shutdown, err := autozpages.Setup(m.TracerProvider())
		if err != nil {
			return errors.Wrap(err, "setup zPages")
		}
		defer func() {
			_ = shutdown(context.Background())
		}()
		if os.Getenv("OTEL_LOGS_EXPORTER") == "otlp" {
			// Setting zap -> otel.
			otelOptions := []otelgrpc.Option{
				otelgrpc.WithTracerProvider(m.TracerProvider()),
				otelgrpc.WithMeterProvider(m.MeterProvider()),
			}
			// Only PoC, should be replaced with real initialization
			// and moved to go-faster/sdk.
			endpoint := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT")
			if endpoint == "" {
				endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
			}
			if endpoint == "" {
				endpoint = "localhost:4317"
			}
			endpoint = strings.TrimPrefix(endpoint, "http://")
			conn, err := grpc.DialContext(ctx, endpoint,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelOptions...)),
			)
			if err != nil {
				return errors.Wrap(err, "dial logs endpoint")
			}
			res, err := app.Resource(ctx)
			if err != nil {
				return errors.Wrap(err, "get resource")
			}
			otelCore := zapotel.New(lg.Level(), res, plogotlp.NewGRPCClient(conn))
			// Update logger down the stack.
			lg.Info("Setting up OTLP log exporter")
			lg = lg.WithOptions(
				zap.WrapCore(func(core zapcore.Core) zapcore.Core {
					return zapcore.NewTee(core, otelCore)
				}),
			)
			ctx = zctx.Base(ctx, lg)
		}

		root, err := newApp(ctx, m)
		if err != nil {
			return errors.Wrap(err, "setup")
		}
		return root.Run(ctx)
	})
}
