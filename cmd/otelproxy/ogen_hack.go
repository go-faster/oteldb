package main

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/go-faster/sdk/app"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

// OgenServer is a generic ogen server type.
type OgenServer[R Route] interface {
	FindPath(method string, u *url.URL) (r R, _ bool)
}

// Route is a generic ogen route type.
type Route interface {
	Name() string
	OperationID() string
}

// RouteFinder finds Route by given URL.
type RouteFinder func(method string, u *url.URL) (Route, bool)

// makeRouteFinder creates RouteFinder from given server.
func makeRouteFinder[R Route, S OgenServer[R]](server S) RouteFinder {
	return func(method string, u *url.URL) (Route, bool) {
		return server.FindPath(method, u)
	}
}

// ServiceMiddleware is a generic middleware for any service.
func ServiceMiddleware(s service, lg *zap.Logger, m *app.Metrics) http.Handler {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			opID   = zap.Skip()
			opName = zap.Skip()
		)
		if route, ok := s.findRoute(r.Method, r.URL); ok {
			opID = zap.String("operationId", route.OperationID())
			opName = zap.String("operationName", route.Name())
		}
		lg.Info("Got request",
			zap.String("method", r.Method),
			zap.Stringer("url", r.URL),
			opID,
			opName,
		)
		s.handler.ServeHTTP(w, r)
	})
	spanFormatter := func(operation string, r *http.Request) string {
		if route, ok := s.findRoute(r.Method, r.URL); ok {
			return fmt.Sprintf("%s.%s", operation, route.OperationID())
		}
		return operation
	}
	return otelhttp.NewHandler(h, fmt.Sprintf("%s.request", s.name),
		otelhttp.WithSpanNameFormatter(spanFormatter),
		otelhttp.WithTracerProvider(m.TracerProvider()),
		otelhttp.WithMeterProvider(m.MeterProvider()),
		otelhttp.WithServerName(s.name),
	)
}
