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

// GetLabelValues implements getLabelValues operation.
// GET /api/v1/label/{label}/values
func (s *Server) GetLabelValues(ctx context.Context, params promapi.GetLabelValuesParams) (*promapi.LabelValuesResponse, error) {
	return s.api.GetLabelValues(ctx, params)
}

// GetLabels implements getLabels operation.
//
// GET /api/v1/labels
func (s *Server) GetLabels(ctx context.Context, params promapi.GetLabelsParams) (*promapi.LabelsResponse, error) {
	return s.api.GetLabels(ctx, params)
}

// GetMetadata implements getMetadata operation.
//
// GET /api/v1/metadata
func (s *Server) GetMetadata(ctx context.Context, params promapi.GetMetadataParams) (*promapi.MetadataResponse, error) {
	return s.api.GetMetadata(ctx, params)
}

// GetQuery implements getQuery operation.
//
// Query Prometheus.
//
// GET /api/v1/query
func (s *Server) GetQuery(ctx context.Context, params promapi.GetQueryParams) (*promapi.QueryResponse, error) {
	return s.api.GetQuery(ctx, params)
}

// GetQueryExemplars implements getQueryExemplars operation.
//
// Query Prometheus.
//
// GET /api/v1/query_examplars
func (s *Server) GetQueryExemplars(ctx context.Context, params promapi.GetQueryExemplarsParams) (*promapi.QueryExemplarsResponse, error) {
	return s.api.GetQueryExemplars(ctx, params)
}

// GetQueryRange implements getQueryRange operation.
//
// Query Prometheus.
//
// GET /api/v1/query_range
func (s *Server) GetQueryRange(ctx context.Context, params promapi.GetQueryRangeParams) (*promapi.QueryResponse, error) {
	return s.api.GetQueryRange(ctx, params)
}

// GetRules implements getRules operation.
//
// GET /api/v1/rules
func (s *Server) GetRules(ctx context.Context, params promapi.GetRulesParams) (*promapi.RulesResponse, error) {
	return s.api.GetRules(ctx, params)
}

// GetSeries implements getSeries operation.
// Query Prometheus.
//
// GET /api/v1/series
func (s *Server) GetSeries(ctx context.Context, params promapi.GetSeriesParams) (*promapi.SeriesResponse, error) {
	return s.api.GetSeries(ctx, params)
}

// PostLabels implements postLabels operation.
//
// POST /api/v1/labels
func (s *Server) PostLabels(ctx context.Context) (*promapi.LabelsResponse, error) {
	return s.api.PostLabels(ctx)
}

// PostQuery implements postQuery operation.
//
// Query Prometheus.
//
// POST /api/v1/query
func (s *Server) PostQuery(ctx context.Context, req *promapi.QueryForm) (*promapi.QueryResponse, error) {
	return s.api.PostQuery(ctx, req)
}

// PostQueryExemplars implements postQueryExemplars operation.
//
// Query Prometheus.
//
// POST /api/v1/query_examplars
func (s *Server) PostQueryExemplars(ctx context.Context) (*promapi.QueryExemplarsResponse, error) {
	return s.api.PostQueryExemplars(ctx)
}

// PostQueryRange implements postQueryRange operation.
//
// Query Prometheus.
//
// POST /api/v1/query_range
func (s *Server) PostQueryRange(ctx context.Context) (*promapi.QueryResponse, error) {
	return s.api.PostQueryRange(ctx)
}

// PostSeries implements postSeries operation.
//
// Query Prometheus.
//
// POST /api/v1/series
func (s *Server) PostSeries(ctx context.Context) (*promapi.SeriesResponse, error) {
	return s.api.PostSeries(ctx)
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
