package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promproxy"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		promURL := os.Getenv("PROMETHEUS_URL")
		promClient, err := promapi.NewClient(promURL,
			promapi.WithTracerProvider(m.TracerProvider()),
			promapi.WithMeterProvider(m.MeterProvider()),
		)
		if err != nil {
			return errors.Wrap(err, "create prometheus client")
		}
		promServer, err := promapi.NewServer(promproxy.NewServer(promClient),
			promapi.WithTracerProvider(m.TracerProvider()),
			promapi.WithMeterProvider(m.MeterProvider()),
		)
		if err != nil {
			return errors.Wrap(err, "create prometheus server")
		}
		addr := os.Getenv("HTTP_ADDR")
		if addr == "" {
			// Default Prometheus port.
			addr = ":9090"
		}
		httpServer := &http.Server{
			Addr:              addr,
			Handler:           promServer,
			ReadHeaderTimeout: 15 * time.Second,
		}
		lg.Info("Starting HTTP server", zap.String("addr", addr))
		parentCtx := ctx
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			<-ctx.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			return httpServer.Shutdown(ctx)
		})
		g.Go(func() error {
			if err := httpServer.ListenAndServe(); err != nil {
				if errors.Is(err, http.ErrServerClosed) && parentCtx.Err() != nil {
					lg.Info("HTTP server closed gracefully")
					return nil
				}
				return errors.Wrap(err, "http server")
			}
			return nil
		})
		return g.Wait()
	})
}
