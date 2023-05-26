package main

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	ytzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"golang.org/x/sync/errgroup"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"

	"github.com/go-faster/oteldb/internal/otelreceiver"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/ytstore"
)

func setupTempo(
	yc yt.Client,
	table ypath.Path,
	lg *zap.Logger,
	m *app.Metrics,
) (http.Handler, error) {
	tempo := ytstore.NewTempoAPI(yc, table)

	s, err := tempoapi.NewServer(tempo,
		tempoapi.WithTracerProvider(m.TracerProvider()),
		tempoapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return nil, err
	}

	var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCtx := r.Context()
		req := r.WithContext(zctx.Base(reqCtx, lg))
		s.ServeHTTP(w, req)
	})
	h = otelhttp.NewHandler(h, "",
		otelhttp.WithTracerProvider(m.TracerProvider()),
		otelhttp.WithMeterProvider(m.MeterProvider()),
	)
	return h, nil
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		yc, err := ythttp.NewClient(&yt.Config{
			Logger: &ytzap.Logger{L: zctx.From(ctx)},
		})
		if err != nil {
			return errors.Wrap(err, "yt")
		}

		tablePath := ypath.Path("//oteldb").Child("traces")
		store := ytstore.NewStore(yc, tablePath)

		{
			migrateBackoff := backoff.NewExponentialBackOff()
			migrateBackoff.InitialInterval = 2 * time.Second
			migrateBackoff.MaxElapsedTime = time.Minute

			if err := backoff.Retry(func() error {
				err := store.Migrate(ctx)
				if err != nil {
					lg.Error("Migration failed", zap.Error(err))
					// FIXME(tdakkota): client does not return a proper error to check
					//  the error message and there is no specific ErrorCode for this error.
					if !strings.Contains(err.Error(), "no healthy tablet cells") {
						return backoff.Permanent(err)
					}
				}
				return err
			}, migrateBackoff); err != nil {
				return errors.Wrap(err, "migrate")
			}
		}

		recv, err := otelreceiver.NewReceiver(store, otelreceiver.ReceiverConfig{
			Logger:         lg.Named("receiver"),
			TracerProvider: m.TracerProvider(),
			MeterProvider:  m.MeterProvider(),
		})
		if err != nil {
			return errors.Wrap(err, "create OTEL receiver")
		}

		tempo, err := setupTempo(yc, tablePath, lg.Named("tempo"), m)
		if err != nil {
			return errors.Wrap(err, "create Tempo API")
		}

		addr := os.Getenv("HTTP_ADDR")
		if addr == "" {
			// Default Tempo API port.
			addr = ":3200"
		}
		httpServer := &http.Server{
			Addr:              addr,
			Handler:           tempo,
			ReadHeaderTimeout: 15 * time.Second,
		}
		lg.Info("Starting HTTP server", zap.String("addr", addr))

		parentCtx := ctx
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return recv.Run(ctx)
		})
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
