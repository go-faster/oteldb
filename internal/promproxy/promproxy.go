// Package promproxy provides Prometheus proxy for observability and research.
package promproxy

import (
	"context"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/promapi"
)

var _ promapi.Handler = &Server{}

// NewServer initializes new proxy Server from openapi client.
func NewServer(api *promapi.Client) *Server {
	return &Server{
		api: api,
	}
}

// Server implement proxy server.
type Server struct {
	api *promapi.Client
}

// GetQuery implements getQuery operation.
//
// Query Prometheus.
//
// GET /api/v1/query
func (s Server) GetQuery(ctx context.Context, params promapi.GetQueryParams) (*promapi.Success, error) {
	return s.api.GetQuery(ctx, params)
}

// PostQuery invokes postQuery operation.
//
// Query Prometheus.
//
// POST /api/v1/query
func (s Server) PostQuery(ctx context.Context) (*promapi.Success, error) {
	return s.api.PostQuery(ctx)
}

// NewError creates *FailStatusCode from error returned by handler.
//
// Used for common default response.
func (s Server) NewError(_ context.Context, err error) *promapi.FailStatusCode {
	if v, ok := errors.Into[*promapi.FailStatusCode](err); ok {
		// Pass as-is.
		return v
	}
	return &promapi.FailStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response: promapi.Fail{
			Error:     err.Error(),
			ErrorType: promapi.FailErrorTypeInternal,
		},
	}
}
