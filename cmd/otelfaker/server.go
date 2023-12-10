package main

import (
	"context"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func server(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
	g, ctx := errgroup.WithContext(ctx)
	mux := http.NewServeMux()

	requestDurations := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "A histogram of the HTTP request durations in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.1, 1.5, 5),
	})

	registry := prometheus.NewRegistry()
	registry.MustRegister(requestDurations)

	// Expose /metrics HTTP endpoint using the created custom registry.
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	tracer := m.TracerProvider().Tracer("server")
	doProcessing := func(ctx context.Context) {
		ctx, span := tracer.Start(ctx, "doProcessing")
		defer span.End()
		// Sleep some random time.
		duration := time.Duration(100+rand.Intn(100)) * time.Millisecond // #nosec G404
		time.Sleep(duration)
		requestDurations.(prometheus.ExemplarObserver).ObserveWithExemplar(duration.Seconds(), prometheus.Labels{
			"trace_id": span.SpanContext().TraceID().String(),
			"span_id":  span.SpanContext().SpanID().String(),
		})
		zctx.From(ctx).Info("got request", zap.Duration("duration", duration))
	}
	mux.HandleFunc("/api/hello", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "hello")
		defer span.End()
		doProcessing(ctx)
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Addr:              "0.0.0.0:8080",
		ReadHeaderTimeout: time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		Handler: otelhttp.NewHandler(mux, "",
			otelhttp.WithMeterProvider(m.MeterProvider()),
			otelhttp.WithTracerProvider(m.TracerProvider()),
			otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
				if r.URL.Path == "/api/hello" {
					return "hello"
				}
				return "metrics"
			}),
		),
	}
	g.Go(func() error {
		lg.Info("server listening", zap.String("addr", srv.Addr))
		defer lg.Info("server stopped")
		return srv.ListenAndServe()
	})
	g.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(ctx)
	})
	return g.Wait()
}
