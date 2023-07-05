// Package lokihandler provides Loki API implementation.
package lokihandler

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	ht "github.com/ogen-go/ogen/http"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

var _ lokiapi.Handler = (*LokiAPI)(nil)

// LokiAPI implements lokiapi.Handler.
type LokiAPI struct {
	q      logstorage.Querier
	engine *logqlengine.Engine
}

// NewLokiAPI creates new LokiAPI.
func NewLokiAPI(q logstorage.Querier, engine *logqlengine.Engine) *LokiAPI {
	return &LokiAPI{
		q:      q,
		engine: engine,
	}
}

// LabelValues implements labelValues operation.
// Get values of label.
//
// GET /loki/api/v1/label/{name}/values
func (h *LokiAPI) LabelValues(ctx context.Context, params lokiapi.LabelValuesParams) (*lokiapi.Values, error) {
	lg := zctx.From(ctx)

	opts, err := getLabelTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
	)
	if err != nil {
		return nil, errors.Wrap(err, "parse params")
	}

	iter, err := h.q.LabelValues(ctx, params.Name, opts)
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	defer func() {
		_ = iter.Close()
	}()

	var values []string
	if err := iterators.ForEach(iter, func(tag logstorage.Label) error {
		values = append(values, tag.Value)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "map tags")
	}
	lg.Debug("Got tag values",
		zap.String("label_name", params.Name),
		zap.Int("count", len(values)),
	)

	return &lokiapi.Values{
		Status: "success",
		Data:   values,
	}, nil
}

// Labels implements labels operation.
//
// Get labels.
// Used by Grafana to test connection to Loki.
//
// GET /loki/api/v1/labels
func (h *LokiAPI) Labels(ctx context.Context, params lokiapi.LabelsParams) (*lokiapi.Labels, error) {
	lg := zctx.From(ctx)

	opts, err := getLabelTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
	)
	if err != nil {
		return nil, errors.Wrap(err, "parse params")
	}

	names, err := h.q.LabelNames(ctx, opts)
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	lg.Debug("Got label names", zap.Int("count", len(names)))

	return &lokiapi.Labels{
		Status: "success",
		Data:   names,
	}, nil
}

func getLabelTimeRange(
	now time.Time,
	startParam lokiapi.OptLokiTime,
	endParam lokiapi.OptLokiTime,
	sinceParam lokiapi.OptPrometheusDuration,
) (opts logstorage.LabelsOptions, _ error) {
	since := 6 * time.Hour
	if v, ok := sinceParam.Get(); ok {
		d, err := model.ParseDuration(string(v))
		if err != nil {
			return opts, errors.Wrap(err, "parse since")
		}
		since = time.Duration(d)
	}

	endValue := endParam.Or("")
	end, err := parseTimestamp(endValue, now)
	if err != nil {
		return opts, errors.Wrapf(err, "parse end %q", endValue)
	}

	// endOrNow is used to apply a default for the start time or an offset if 'since' is provided.
	// we want to use the 'end' time so long as it's not in the future as this should provide
	// a more intuitive experience when end time is in the future.
	endOrNow := end
	if end.After(now) {
		endOrNow = now
	}

	startValue := startParam.Or("")
	start, err := parseTimestamp(startValue, endOrNow.Add(-since))
	if err != nil {
		return opts, errors.Wrapf(err, "parse start %q", startValue)
	}

	return logstorage.LabelsOptions{
		Start: pcommon.NewTimestampFromTime(start),
		End:   pcommon.NewTimestampFromTime(end),
	}, nil
}

func parseTimestamp(lt lokiapi.LokiTime, def time.Time) (time.Time, error) {
	value := string(lt)
	if value == "" {
		return def, nil
	}

	if strings.Contains(value, ".") {
		if t, err := strconv.ParseFloat(value, 64); err == nil {
			s, ns := math.Modf(t)
			ns = math.Round(ns*1000) / 1000
			return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
		}
	}
	nanos, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		if ts, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return ts, nil
		}
		return time.Time{}, err
	}
	if len(value) <= 10 {
		return time.Unix(nanos, 0), nil
	}
	return time.Unix(0, nanos), nil
}

// Push implements push operation.
//
// Push data.
//
// POST /loki/api/v1/push
func (h *LokiAPI) Push(context.Context, lokiapi.PushReq) error {
	return ht.ErrNotImplemented
}

// Query implements query operation.
//
// Query.
//
// GET /loki/api/v1/query
func (h *LokiAPI) Query(ctx context.Context, params lokiapi.QueryParams) (*lokiapi.QueryResponse, error) {
	lg := zctx.From(ctx)

	ts, err := parseTimestamp(params.Time.Value, time.Now())
	if err != nil {
		return nil, errors.Wrap(err, "parse time")
	}

	streams, err := h.engine.Eval(ctx, params.Query, logqlengine.EvalParams{
		Direction: string(params.Direction.Or("backward")),
		Start:     otelstorage.NewTimestampFromTime(ts),
		End:       otelstorage.NewTimestampFromTime(ts),
		Limit:     params.Limit.Or(100),
	})
	if err != nil {
		return nil, errors.Wrap(err, "eval")
	}
	lg.Debug("Query", zap.Int("streams", len(streams)))

	return &lokiapi.QueryResponse{
		Status: "success",
		Data: lokiapi.QueryResponseData{
			ResultType: lokiapi.QueryResponseDataResultTypeStreams,
			Result:     streams,
		},
	}, nil
}

// QueryRange implements queryRange operation.
//
// Query range.
//
// GET /loki/api/v1/query_range
func (h *LokiAPI) QueryRange(context.Context, lokiapi.QueryRangeParams) (*lokiapi.QueryResponse, error) {
	return nil, ht.ErrNotImplemented
}

// Series implements series operation.
//
// Get series.
//
// GET /loki/api/v1/series
func (h *LokiAPI) Series(context.Context, lokiapi.SeriesParams) (*lokiapi.Maps, error) {
	return nil, ht.ErrNotImplemented
}

// NewError creates *ErrorStatusCode from error returned by handler.
//
// Used for common default response.
func (h *LokiAPI) NewError(_ context.Context, err error) *lokiapi.ErrorStatusCode {
	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   lokiapi.Error(err.Error()),
	}
}
