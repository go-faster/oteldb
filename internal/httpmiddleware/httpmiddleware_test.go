// Package httpmiddleware contains HTTP middlewares.
package httpmiddleware

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/go-faster/sdk/zctx"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
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
	require.Len(t, entries, 2)

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
