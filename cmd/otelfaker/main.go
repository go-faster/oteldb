package main

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		// Generate fake telemetry signals to test oteldb.
		target := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		target = strings.TrimPrefix(target, "http://")
		otelOptions := []otelgrpc.Option{
			otelgrpc.WithTracerProvider(m.TracerProvider()),
			otelgrpc.WithMeterProvider(m.MeterProvider()),
		}
		conn, err := grpc.DialContext(ctx, target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor(otelOptions...)),
			grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor(otelOptions...)),
		)
		if err != nil {
			return errors.Wrap(err, "dial oteldb")
		}
		client := plogotlp.NewGRPCClient(conn)
		for range time.NewTicker(time.Second).C {
			logs := plog.NewLogs()
			if _, err := client.Export(ctx, plogotlp.NewExportRequestFromLogs(logs)); err != nil {
				return errors.Wrap(err, "send logs")
			}
		}
		return nil
	})
}
