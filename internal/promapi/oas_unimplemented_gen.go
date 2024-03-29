// Code generated by ogen, DO NOT EDIT.

package promapi

import (
	"context"

	ht "github.com/ogen-go/ogen/http"
)

// UnimplementedHandler is no-op Handler which returns http.ErrNotImplemented.
type UnimplementedHandler struct{}

var _ Handler = UnimplementedHandler{}

// GetLabelValues implements getLabelValues operation.
//
// GET /api/v1/label/{label}/values
func (UnimplementedHandler) GetLabelValues(ctx context.Context, params GetLabelValuesParams) (r *LabelValuesResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// GetLabels implements getLabels operation.
//
// GET /api/v1/labels
func (UnimplementedHandler) GetLabels(ctx context.Context, params GetLabelsParams) (r *LabelsResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// GetMetadata implements getMetadata operation.
//
// GET /api/v1/metadata
func (UnimplementedHandler) GetMetadata(ctx context.Context, params GetMetadataParams) (r *MetadataResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// GetQuery implements getQuery operation.
//
// Query Prometheus.
//
// GET /api/v1/query
func (UnimplementedHandler) GetQuery(ctx context.Context, params GetQueryParams) (r *QueryResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// GetQueryExemplars implements getQueryExemplars operation.
//
// Query Prometheus.
//
// GET /api/v1/query_exemplars
func (UnimplementedHandler) GetQueryExemplars(ctx context.Context, params GetQueryExemplarsParams) (r *QueryExemplarsResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// GetQueryRange implements getQueryRange operation.
//
// Query Prometheus.
//
// GET /api/v1/query_range
func (UnimplementedHandler) GetQueryRange(ctx context.Context, params GetQueryRangeParams) (r *QueryResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// GetRules implements getRules operation.
//
// GET /api/v1/rules
func (UnimplementedHandler) GetRules(ctx context.Context, params GetRulesParams) (r *RulesResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// GetSeries implements getSeries operation.
//
// Query Prometheus.
//
// GET /api/v1/series
func (UnimplementedHandler) GetSeries(ctx context.Context, params GetSeriesParams) (r *SeriesResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// PostLabels implements postLabels operation.
//
// POST /api/v1/labels
func (UnimplementedHandler) PostLabels(ctx context.Context, req *LabelsForm) (r *LabelsResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// PostQuery implements postQuery operation.
//
// Query Prometheus.
//
// POST /api/v1/query
func (UnimplementedHandler) PostQuery(ctx context.Context, req *QueryForm) (r *QueryResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// PostQueryExemplars implements postQueryExemplars operation.
//
// Query Prometheus.
//
// POST /api/v1/query_exemplars
func (UnimplementedHandler) PostQueryExemplars(ctx context.Context, req *ExemplarsForm) (r *QueryExemplarsResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// PostQueryRange implements postQueryRange operation.
//
// Query Prometheus.
//
// POST /api/v1/query_range
func (UnimplementedHandler) PostQueryRange(ctx context.Context, req *QueryRangeForm) (r *QueryResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// PostSeries implements postSeries operation.
//
// Query Prometheus.
//
// POST /api/v1/series
func (UnimplementedHandler) PostSeries(ctx context.Context, req *SeriesForm) (r *SeriesResponse, _ error) {
	return r, ht.ErrNotImplemented
}

// NewError creates *FailStatusCode from error returned by handler.
//
// Used for common default response.
func (UnimplementedHandler) NewError(ctx context.Context, err error) (r *FailStatusCode) {
	r = new(FailStatusCode)
	return r
}
