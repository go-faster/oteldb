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

type Server struct {
	api *promapi.Client
}

func (s Server) GetQuery(ctx context.Context, params promapi.GetQueryParams) (*promapi.Success, error) {
	return s.api.GetQuery(ctx, params)
}

func (s Server) PostQuery(ctx context.Context) (*promapi.Success, error) {
	return s.api.PostQuery(ctx)
}

func (s Server) NewError(ctx context.Context, err error) *promapi.FailStatusCode {
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
