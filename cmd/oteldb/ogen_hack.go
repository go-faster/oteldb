package main

import (
	"net/http"
	"net/url"

	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

func injectLogger(h http.Handler, lg *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCtx := r.Context()
		req := r.WithContext(zctx.Base(reqCtx, lg))
		h.ServeHTTP(w, req)
	})
}

func instrumentHTTP[R interface {
	OperationID() string
}](
	h http.Handler,
	findRoute func(string, *url.URL) (R, bool),
	lg *zap.Logger,
	m Metrics,
) http.Handler {
	h = injectLogger(h, lg)
	return otelhttp.NewHandler(h, "",
		otelhttp.WithTracerProvider(m.TracerProvider()),
		otelhttp.WithMeterProvider(m.MeterProvider()),
		otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
			op, ok := findRoute(r.Method, r.URL)
			if ok {
				return "http." + op.OperationID()
			}
			return operation
		}),
	)
}
