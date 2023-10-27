package promhandler

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/oteldb/internal/promapi"
	ht "github.com/ogen-go/ogen/http"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/exp/maps"
)

var _ promapi.Handler = (*PromAPI)(nil)

// Engine is a Prometheus engine interface.
type Engine interface {
	NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error)
	NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error)
}

// PromAPI implements promapi.Handler.
type PromAPI struct {
	eng   Engine
	store storage.Queryable
}

// NewPromAPI creates new PromAPI.
func NewPromAPI(eng Engine, store storage.Queryable) *PromAPI {
	return &PromAPI{
		eng:   eng,
		store: store,
	}
}

// GetLabelValues implements getLabelValues operation.
// GET /api/v1/label/{label}/values
func (h *PromAPI) GetLabelValues(ctx context.Context, params promapi.GetLabelValuesParams) (*promapi.LabelValuesResponse, error) {
	mint, err := parseTimestamp(params.Start, 0)
	if err != nil {
		return nil, errors.Wrap(err, "parse start")
	}
	maxt, err := parseTimestamp(params.End, 0)
	if err != nil {
		return nil, errors.Wrap(err, "parse end")
	}
	sets, err := parseLabelMatchers(params.Match)
	if err != nil {
		return nil, errors.Wrap(err, "parse match")
	}

	q, err := h.store.Querier(mint, maxt)
	if err != nil {
		return nil, errors.Wrap(err, "get querier")
	}

	// Fast path for cases when match[] is not set.
	if len(sets) < 2 {
		var matchers []*labels.Matcher
		if len(sets) > 0 {
			matchers = sets[0]
		}

		values, warnings, err := q.LabelValues(ctx, params.Label, matchers...)
		if err != nil {
			return nil, errors.Wrap(err, "label values")
		}

		return &promapi.LabelValuesResponse{
			Status:   "success",
			Warnings: warnings.AsStrings("", 0),
			Data:     values,
		}, nil
	}

	var (
		data     = map[string]struct{}{}
		warnings annotations.Annotations
	)
	for _, set := range sets {
		vals, w, err := q.LabelValues(ctx, params.Label, set...)
		if err != nil {
			return nil, errors.Wrapf(err, "get label values for set %+v", set)
		}

		for _, val := range vals {
			data[val] = struct{}{}
		}
		warnings = warnings.Merge(w)
	}

	return &promapi.LabelValuesResponse{
		Status:   "success",
		Warnings: warnings.AsStrings("", 0),
		Data:     maps.Keys(data),
	}, nil
}

// GetLabels implements getLabels operation.
//
// GET /api/v1/labels
func (h *PromAPI) GetLabels(ctx context.Context, params promapi.GetLabelsParams) (*promapi.LabelsResponse, error) {
	mint, err := parseTimestamp(params.Start, 0)
	if err != nil {
		return nil, errors.Wrap(err, "parse start")
	}
	maxt, err := parseTimestamp(params.End, 0)
	if err != nil {
		return nil, errors.Wrap(err, "parse end")
	}
	sets, err := parseLabelMatchers(params.Match)
	if err != nil {
		return nil, errors.Wrap(err, "parse match")
	}

	q, err := h.store.Querier(mint, maxt)
	if err != nil {
		return nil, errors.Wrap(err, "get querier")
	}

	// Fast path for cases when match[] is not set.
	if len(sets) < 2 {
		var matchers []*labels.Matcher
		if len(sets) > 0 {
			matchers = sets[0]
		}

		values, warnings, err := q.LabelNames(ctx, matchers...)
		if err != nil {
			return nil, errors.Wrap(err, "label names")
		}

		return &promapi.LabelsResponse{
			Status:   "success",
			Warnings: warnings.AsStrings("", 0),
			Data:     values,
		}, nil
	}

	var (
		data     = map[string]struct{}{}
		warnings annotations.Annotations
	)
	for _, set := range sets {
		vals, w, err := q.LabelNames(ctx, set...)
		if err != nil {
			return nil, errors.Wrapf(err, "get label names for set %+v", set)
		}

		for _, val := range vals {
			data[val] = struct{}{}
		}
		warnings = warnings.Merge(w)
	}

	return &promapi.LabelsResponse{
		Status:   "success",
		Warnings: warnings.AsStrings("", 0),
		Data:     maps.Keys(data),
	}, nil
}

// GetMetadata implements getMetadata operation.
//
// GET /api/v1/metadata
func (h *PromAPI) GetMetadata(ctx context.Context, params promapi.GetMetadataParams) (*promapi.MetadataResponse, error) {
	return nil, ht.ErrNotImplemented
}

// GetQuery implements getQuery operation.
//
// Query Prometheus.
//
// GET /api/v1/query
func (h *PromAPI) GetQuery(ctx context.Context, params promapi.GetQueryParams) (*promapi.QueryResponse, error) {
	return nil, ht.ErrNotImplemented
}

// GetQueryExemplars implements getQueryExemplars operation.
//
// Query Prometheus.
//
// GET /api/v1/query_exemplars
func (h *PromAPI) GetQueryExemplars(ctx context.Context, params promapi.GetQueryExemplarsParams) (*promapi.QueryExemplarsResponse, error) {
	return nil, ht.ErrNotImplemented
}

// GetQueryRange implements getQueryRange operation.
//
// Query Prometheus.
//
// GET /api/v1/query_range
func (h *PromAPI) GetQueryRange(ctx context.Context, params promapi.GetQueryRangeParams) (*promapi.QueryResponse, error) {
	return nil, ht.ErrNotImplemented
}

// GetRules implements getRules operation.
//
// GET /api/v1/rules
func (h *PromAPI) GetRules(ctx context.Context, params promapi.GetRulesParams) (*promapi.RulesResponse, error) {
	return nil, ht.ErrNotImplemented
}

// GetSeries implements getSeries operation.
//
// Query Prometheus.
//
// GET /api/v1/series
func (h *PromAPI) GetSeries(ctx context.Context, params promapi.GetSeriesParams) (*promapi.SeriesResponse, error) {
	return nil, ht.ErrNotImplemented
}

// PostLabels implements postLabels operation.
//
// POST /api/v1/labels
func (h *PromAPI) PostLabels(ctx context.Context) (*promapi.LabelsResponse, error) {
	return nil, ht.ErrNotImplemented
}

// PostQuery implements postQuery operation.
//
// Query Prometheus.
//
// POST /api/v1/query
func (h *PromAPI) PostQuery(ctx context.Context, req *promapi.QueryForm) (*promapi.QueryResponse, error) {
	return nil, ht.ErrNotImplemented
}

// PostQueryExemplars implements postQueryExemplars operation.
//
// Query Prometheus.
//
// POST /api/v1/query_exemplars
func (h *PromAPI) PostQueryExemplars(ctx context.Context) (*promapi.QueryExemplarsResponse, error) {
	return nil, ht.ErrNotImplemented
}

// PostQueryRange implements postQueryRange operation.
//
// Query Prometheus.
//
// POST /api/v1/query_range
func (h *PromAPI) PostQueryRange(ctx context.Context, req *promapi.QueryRangeForm) (*promapi.QueryResponse, error) {
	return nil, ht.ErrNotImplemented
}

// PostSeries implements postSeries operation.
//
// Query Prometheus.
//
// POST /api/v1/series
func (h *PromAPI) PostSeries(ctx context.Context) (*promapi.SeriesResponse, error) {
	return nil, ht.ErrNotImplemented
}

// NewError creates *FailStatusCode from error returned by handler.
//
// Used for common default response.
func (h *PromAPI) NewError(ctx context.Context, err error) *promapi.FailStatusCode {
	// TODO: handle properly like
	// 	https://github.com/prometheus/prometheus/blob/05356e76de82724ebf104457cb4cf1e91920621e/web/api/v1/api.go#L1737
	//
	return &promapi.FailStatusCode{
		StatusCode: http.StatusBadRequest,
		Response: promapi.Fail{
			Status: "error",
			Error:  err.Error(),
		},
	}
}

func parseLabelMatchers(matchers []string) ([][]*labels.Matcher, error) {
	var matcherSets [][]*labels.Matcher
	for _, s := range matchers {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, err
		}
		matcherSets = append(matcherSets, matchers)
	}

OUTER:
	for _, ms := range matcherSets {
		for _, lm := range ms {
			if lm != nil && !lm.Matches("") {
				continue OUTER
			}
		}
		return nil, errors.New("match[] must contain at least one non-empty matcher")
	}
	return matcherSets, nil
}

func parseTimestamp(t promapi.OptPrometheusTimestamp, or int64) (int64, error) {
	v, ok := t.Get()
	if !ok {
		return or, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, string(v)); err == nil {
		return t.UnixMilli(), nil
	}
	return strconv.ParseInt(string(v), 10, 64)
}
