// Package lokihandler provides Loki API implementation.
package lokihandler

import (
	"context"
	"net/http"
	"time"

	ht "github.com/ogen-go/ogen/http"
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

// IndexStats implements indexStats operation.
//
// Get index stats.
//
// GET /loki/api/v1/index/stats
func (h *LokiAPI) IndexStats(context.Context, lokiapi.IndexStatsParams) (*lokiapi.IndexStats, error) {
	// No stats for now.
	return &lokiapi.IndexStats{}, nil
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

	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
	)
	if err != nil {
		return nil, errors.Wrap(err, "parse time range")
	}

	iter, err := h.q.LabelValues(ctx, params.Name, logstorage.LabelsOptions{
		Start: otelstorage.NewTimestampFromTime(start),
		End:   otelstorage.NewTimestampFromTime(end),
	})
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

	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
	)
	if err != nil {
		return nil, errors.Wrap(err, "parse time range")
	}

	names, err := h.q.LabelNames(ctx, logstorage.LabelsOptions{
		Start: otelstorage.NewTimestampFromTime(start),
		End:   otelstorage.NewTimestampFromTime(end),
	})
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	lg.Debug("Got label names", zap.Int("count", len(names)))

	return &lokiapi.Labels{
		Status: "success",
		Data:   names,
	}, nil
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

	data, err := h.engine.Eval(ctx, params.Query, logqlengine.EvalParams{
		Start:     otelstorage.NewTimestampFromTime(ts),
		End:       otelstorage.NewTimestampFromTime(ts),
		Step:      0,
		Direction: string(params.Direction.Or(lokiapi.DirectionBackward)),
		Limit:     params.Limit.Or(100),
	})
	if err != nil {
		return nil, errors.Wrap(err, "eval")
	}
	lg.Debug("Query", zap.String("type", string(data.Type)))

	return &lokiapi.QueryResponse{
		Status: "success",
		Data:   data,
	}, nil
}

// QueryRange implements queryRange operation.
//
// Query range.
//
// GET /loki/api/v1/query_range
func (h *LokiAPI) QueryRange(ctx context.Context, params lokiapi.QueryRangeParams) (*lokiapi.QueryResponse, error) {
	lg := zctx.From(ctx)

	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
	)
	if err != nil {
		return nil, errors.Wrap(err, "parse time range")
	}

	step, err := parseStep(params.Step, start, end)
	if err != nil {
		return nil, errors.Wrap(err, "parse step")
	}

	data, err := h.engine.Eval(ctx, params.Query, logqlengine.EvalParams{
		Start:     otelstorage.NewTimestampFromTime(start),
		End:       otelstorage.NewTimestampFromTime(end),
		Step:      step,
		Direction: string(params.Direction.Or(lokiapi.DirectionBackward)),
		Limit:     params.Limit.Or(100),
	})
	if err != nil {
		return nil, errors.Wrap(err, "eval")
	}
	lg.Debug("Query range", zap.String("type", string(data.Type)))

	return &lokiapi.QueryResponse{
		Status: "success",
		Data:   data,
	}, nil
}

// Series implements series operation.
//
// Get series.
//
// GET /loki/api/v1/series
func (h *LokiAPI) Series(ctx context.Context, params lokiapi.SeriesParams) (*lokiapi.Maps, error) {
	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
	)
	if err != nil {
		return nil, errors.Wrap(err, "parse time range")
	}
	out := make([]lokiapi.MapsDataItem, 0, len(params.Match))
	zctx.From(ctx).Info("Series", zap.Int("match", len(params.Match)))
	for _, q := range params.Match {
		// TODO(ernado): offload
		data, err := h.engine.Eval(ctx, q, logqlengine.EvalParams{
			Start:     otelstorage.NewTimestampFromTime(start),
			End:       otelstorage.NewTimestampFromTime(end),
			Direction: string(lokiapi.DirectionBackward),
			Limit:     1_000,
		})
		if err != nil {
			return nil, errors.Wrap(err, "eval")
		}
		if streams, ok := data.GetStreamsResult(); ok {
			for _, stream := range streams.Result {
				if labels, ok := stream.Stream.Get(); ok {
					// TODO(ernado): should be MapsDataItem 1:1 match?
					out = append(out, lokiapi.MapsDataItem(labels))
				}
			}
		}
	}
	return &lokiapi.Maps{
		Status: "success",
		Data:   out,
	}, nil
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
