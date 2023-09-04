// TODO(tdakkota): move to go-faster/sdk?

// Package autozpages setups zPages handler.
package autozpages

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/contrib/zpages"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// ShutdownFunc is a function that shuts down the MeterProvider.
type ShutdownFunc func(ctx context.Context) error

func noop(_ context.Context) error { return nil }

func zpagesAddr() string {
	host := "localhost"
	port := "55679"
	if v := os.Getenv("OTEL_ZPAGES_HOST"); v != "" {
		host = v
	}
	if v := os.Getenv("OTEL_ZPAGES_PORT"); v != "" {
		port = v
	}
	return net.JoinHostPort(host, port)
}

// RegisterableTracerProvider is a Tracer provider.
type RegisterableTracerProvider interface {
	// RegisterSpanProcessor adds the given SpanProcessor to the list of SpanProcessors.
	// https://pkg.go.dev/go.opentelemetry.io/otel/sdk/trace#TracerProvider.RegisterSpanProcessor.
	RegisterSpanProcessor(SpanProcessor sdktrace.SpanProcessor)

	// UnregisterSpanProcessor removes the given SpanProcessor from the list of SpanProcessors.
	// https://pkg.go.dev/go.opentelemetry.io/otel/sdk/trace#TracerProvider.UnregisterSpanProcessor.
	UnregisterSpanProcessor(SpanProcessor sdktrace.SpanProcessor)
}

// Setup setups zPages.
func Setup(provider trace.TracerProvider) (ShutdownFunc, error) {
	if _, ok := os.LookupEnv("OTELDB_ZPAGES"); !ok {
		return noop, nil
	}

	reg, ok := provider.(RegisterableTracerProvider)
	if !ok {
		return noop, nil
	}

	return NewZPages(reg)
}

// NewZPages registers zPages processor on given trace provider and runs HTTP server.
func NewZPages(provider RegisterableTracerProvider) (_ ShutdownFunc, rerr error) {
	proc := zpages.NewSpanProcessor()

	provider.RegisterSpanProcessor(proc)
	// Unregister processor if setup fails.
	defer func() {
		if rerr != nil {
			provider.UnregisterSpanProcessor(proc)
		}
	}()

	addr := zpagesAddr()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.Wrapf(err, "listen zPages addr %q", addr)
	}
	// Close listener if setup fails.
	defer func() {
		if rerr != nil {
			_ = ln.Close()
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/tracez", zpages.NewTracezHandler(proc))

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		_ = srv.Serve(ln)
	}()

	return func(ctx context.Context) (rerr error) {
		rerr = srv.Shutdown(ctx)
		provider.UnregisterSpanProcessor(proc)
		return rerr
	}, nil
}
