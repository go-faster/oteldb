// Package httpmiddleware contains HTTP middlewares.
package httpmiddleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-faster/sdk/zctx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-faster/oteldb/integration"
)

type testHandler struct{}

func (*testHandler) ServeHTTP(http.ResponseWriter, *http.Request) {}

type testMiddleware struct{}

func (*testMiddleware) ServeHTTP(http.ResponseWriter, *http.Request) {}

func TestInjectLogger(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)

	h := Wrap(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			lg := zctx.From(r.Context())
			lg.Info("Hello")
		}),
		InjectLogger(zap.New(core)),
	)
	h.ServeHTTP(nil, &http.Request{})

	entries := logs.All()
	require.Len(t, entries, 1)

	entry := entries[0]
	require.Equal(t, "Hello", entry.Message)
}

type testOgenServer struct{}

func (*testOgenServer) FindPath(method string, u *url.URL) (r testOgenRoute, _ bool) {
	if method != http.MethodGet || u.Path != "/foo" {
		return r, false
	}
	return r, true
}

type testOgenRoute struct{}

func (testOgenRoute) Name() string        { return "TestOgenRoute" }
func (testOgenRoute) OperationID() string { return "testOgenRoute" }

func TestLogRequests(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)

	h := Wrap(
		&testHandler{},
		InjectLogger(zap.New(core)),
		LogRequests(MakeRouteFinder(&testOgenServer{})),
	)
	h.ServeHTTP(nil, &http.Request{
		Method: http.MethodPost,
		URL: &url.URL{
			Path: "/unknown_ogen_path",
		},
	})
	h.ServeHTTP(nil, &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path: "/foo",
		},
	})

	entries := logs.All()
	require.Len(t, entries, 4)

	entry := entries[0]
	require.Equal(t, "Got request", entry.Message)
	fields := entry.ContextMap()
	require.Len(t, fields, 2)
	require.Equal(t, http.MethodPost, fields["method"])
	require.Equal(t, "/unknown_ogen_path", fields["url"])

	entry = entries[1]
	require.Equal(t, "Got request", entry.Message)
	fields = entry.ContextMap()
	require.Len(t, fields, 4)
	require.Equal(t, http.MethodGet, fields["method"])
	require.Equal(t, "/foo", fields["url"])
	require.Equal(t, "TestOgenRoute", fields["operationName"])
	require.Equal(t, "testOgenRoute", fields["operationId"])
}

func TestWrap(t *testing.T) {
	endpoint := &testHandler{}

	// Check case with zero middlewares.
	result := Wrap(endpoint)
	require.Equal(t, endpoint, result)

	// Check case with one middleware.
	middleware := &testMiddleware{}
	result = Wrap(endpoint, func(h http.Handler) http.Handler {
		return middleware
	})
	require.Equal(t, middleware, result)

	// Ensure order of wrapping.
	var (
		calls          []int
		callMiddleware = func(n int) Middleware {
			return func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls = append(calls, n)
					next.ServeHTTP(w, r)
				})
			}
		}
	)

	result = Wrap(endpoint,
		callMiddleware(1),
		callMiddleware(2),
		callMiddleware(3),
	)
	result.ServeHTTP(nil, nil)
	require.Equal(t, []int{1, 2, 3}, calls)
}

func TestInstrumentation(t *testing.T) {
	provider := integration.NewProvider()
	tracer := provider.Tracer("test")
	fn := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		t.Logf("Handler(ctx).IsValid(): %v", trace.SpanContextFromContext(ctx).IsValid())
		assert.True(t, trace.SpanContextFromContext(ctx).IsValid())
		_, span := tracer.Start(r.Context(), "Handler")
		defer span.End()
		w.WriteHeader(http.StatusOK)
	})
	h := Wrap(fn,
		otelhttp.NewMiddleware("otelhttp.Middleware",
			otelhttp.WithTracerProvider(provider),
		),
		func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				sc := trace.SpanContextFromContext(ctx)
				t.Logf("after(ctx).IsValid(): %v (%s)", sc.IsValid(), sc.TraceID())
				assert.True(t, sc.IsValid(), "Middleware span should be valid")

				ctx, span := tracer.Start(ctx, "After")
				defer span.End()
				handler.ServeHTTP(w, r.WithContext(ctx))
			})
		},
	)
	rw := httptest.NewRecorder()
	req := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path: "/foo",
		},
	}
	h.ServeHTTP(rw, req.WithContext(context.Background()))
	require.Equal(t, http.StatusOK, rw.Code)
	provider.Flush()
	spans := provider.Exporter.GetSpans()
	assert.Len(t, spans, 3)
	for _, s := range spans {
		t.Logf("%s [%s]", s.Name, s.SpanContext.TraceID())
	}
}
