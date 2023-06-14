// Package lokiproxy provides Loki proxy for observability and research.
package lokiproxy

import (
	"context"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/lokiapi"
)

var _ lokiapi.Handler = &Server{}

// NewServer initializes new proxy Server from openapi client.
func NewServer(api *lokiapi.Client) *Server {
	return &Server{api: api}
}

// Server implement proxy server.
type Server struct {
	api *lokiapi.Client
}

// GetLabelValues implements GetLabelValues operation.
// Get values of label.
//
// GET /loki/api/v1/label/{name}/values
func (s *Server) GetLabelValues(ctx context.Context, params lokiapi.GetLabelValuesParams) (*lokiapi.Values, error) {
	return s.api.GetLabelValues(ctx, params)
}

// GetLabels implements GetLabels operation.
//
// Get labels.
// Used by Grafana to test connection to Loki.
//
// GET /loki/api/v1/labels
func (s *Server) GetLabels(ctx context.Context, params lokiapi.GetLabelsParams) (*lokiapi.Labels, error) {
	return s.api.GetLabels(ctx, params)
}

// QueryRange implements QueryRange operation.
//
// Query range.
//
// GET /loki/api/v1/query_range
func (s *Server) QueryRange(ctx context.Context, params lokiapi.QueryRangeParams) (*lokiapi.QueryResponse, error) {
	return s.api.QueryRange(ctx, params)
}

// Series implements Series operation.
//
// Get series.
//
// GET /loki/api/v1/series
func (s *Server) Series(ctx context.Context, params lokiapi.SeriesParams) (*lokiapi.Maps, error) {
	return s.api.Series(ctx, params)
}

// NewError creates *ErrorStatusCode from error returned by handler.
//
// Used for common default response.
func (s *Server) NewError(_ context.Context, err error) *lokiapi.ErrorStatusCode {
	if v, ok := errors.Into[*lokiapi.ErrorStatusCode](err); ok {
		// Pass as-is.
		return v
	}
	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   lokiapi.Error(err.Error()),
	}
}
