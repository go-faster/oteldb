// Package logqlengine implements LogQL evaluation engine.
package logqlengine

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Engine is a LogQL evaluation engine.
type Engine struct {
	querier     Querier
	querierCaps QuerierCapabilities

	lookbackDuration time.Duration
	parseOpts        logql.ParseOptions

	tracer trace.Tracer
}

// Options sets Engine options.
type Options struct {
	// LookbackDuration sets lookback duration for instant queries.
	//
	// Should be negative, otherwise default value would be used.
	LookbackDuration time.Duration

	// ParseOptions is a LogQL parser options.
	ParseOptions logql.ParseOptions

	// TracerProvider provides OpenTelemetry tracer for this engine.
	TracerProvider trace.TracerProvider
}

func (o *Options) setDefaults() {
	if o.LookbackDuration >= 0 {
		o.LookbackDuration = -30 * time.Second
	}
	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}
}

// NewEngine creates new Engine.
func NewEngine(querier Querier, opts Options) *Engine {
	opts.setDefaults()

	return &Engine{
		querier:          querier,
		querierCaps:      querier.Capabilities(),
		lookbackDuration: opts.LookbackDuration,
		parseOpts:        opts.ParseOptions,
		tracer:           opts.TracerProvider.Tracer("logql.Engine"),
	}
}

// EvalParams sets evaluation parameters.
type EvalParams struct {
	Start     otelstorage.Timestamp
	End       otelstorage.Timestamp
	Step      time.Duration
	Direction Direction // forward, backward
	Limit     int
}

// IsInstant whether query is instant.
func (p EvalParams) IsInstant() bool {
	return p.Start == p.End && p.Step == 0
}

// Direction describe log ordering.
type Direction string

const (
	// DirectionBackward sorts records in descending order.
	DirectionBackward Direction = "backward"
	// DirectionForward sorts records in ascending order.
	DirectionForward Direction = "forward"
)

// Eval parses and evaluates query.
func (e *Engine) Eval(ctx context.Context, query string, params EvalParams) (data lokiapi.QueryResponseData, rerr error) {
	ctx, span := e.tracer.Start(ctx, "Eval",
		trace.WithAttributes(
			attribute.String("logql.query", query),
			attribute.Int64("logql.start", int64(params.Start)),
			attribute.Int64("logql.end", int64(params.End)),
			attribute.Int64("logql.step", int64(params.Step)),
			attribute.String("logql.direction", string(params.Direction)),
			attribute.Int("logql.limit", params.Limit),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else if streams, ok := data.GetStreamsResult(); ok {
			var entries int
			for _, stream := range streams.Result {
				entries += len(stream.Values)
			}
			span.SetAttributes(
				attribute.Int("logql.returned_entries", entries),
				attribute.Int("logql.returned_streams", len(streams.Result)),
			)
		}
		span.End()
	}()

	expr, err := logql.Parse(query, e.parseOpts)
	if err != nil {
		return data, errors.Wrap(err, "parse")
	}

	return e.evalExpr(ctx, expr, params)
}

func (e *Engine) evalExpr(ctx context.Context, expr logql.Expr, params EvalParams) (data lokiapi.QueryResponseData, _ error) {
	ctx, span := e.tracer.Start(ctx, "evalExpr",
		trace.WithAttributes(
			attribute.String("logql.expr", fmt.Sprintf("%T", expr)),
		),
	)
	defer span.End()
	switch expr := logql.UnparenExpr(expr).(type) {
	case *logql.LogExpr:
		streams, err := e.evalLogExpr(ctx, expr, params)
		if err != nil {
			return data, errors.Wrap(err, "evaluate log query")
		}

		data.SetStreamsResult(lokiapi.StreamsResult{
			Result: streams,
		})
		return data, nil
	case *logql.LiteralExpr:
		return e.evalLiteral(expr, params), nil
	case logql.MetricExpr:
		iter, err := logqlmetric.Build(expr, e.sampleSelector(ctx, params), logqlmetric.EvalParams{
			Start: params.Start.AsTime(),
			End:   params.End.AsTime(),
			Step:  params.Step,
		})
		if err != nil {
			return data, errors.Wrap(err, "build metric query")
		}

		data, err = logqlmetric.ReadStepResponse(iter, params.IsInstant())
		if err != nil {
			return data, errors.Wrap(err, "evaluate metric query")
		}

		return data, nil
	default:
		return data, errors.Errorf("unexpected expression %T", expr)
	}
}

func addDuration(ts otelstorage.Timestamp, d time.Duration) otelstorage.Timestamp {
	return otelstorage.NewTimestampFromTime(ts.AsTime().Add(d))
}

func (e *Engine) evalLiteral(expr *logql.LiteralExpr, params EvalParams) (data lokiapi.QueryResponseData) {
	if params.IsInstant() {
		data.SetScalarResult(lokiapi.ScalarResult{
			Result: lokiapi.FPoint{
				T: getPrometheusTimestamp(params.Start.AsTime()),
				V: strconv.FormatFloat(expr.Value, 'f', -1, 64),
			},
		})
		return data
	}
	data.SetMatrixResult(lokiapi.MatrixResult{
		Result: generateLiteralMatrix(expr.Value, params),
	})
	return data
}

func generateLiteralMatrix(value float64, params EvalParams) lokiapi.Matrix {
	var (
		start = params.Start.AsTime()
		end   = params.End.AsTime()

		series = lokiapi.Series{
			Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet{}),
		}
	)

	strValue := strconv.FormatFloat(value, 'f', -1, 64)
	for ts := start; ts.Equal(end) || ts.Before(end); ts = ts.Add(params.Step) {
		series.Values = append(series.Values, lokiapi.FPoint{
			T: getPrometheusTimestamp(ts),
			V: strValue,
		})
	}
	return lokiapi.Matrix{series}
}

func getPrometheusTimestamp(t time.Time) float64 {
	// Pass milliseconds as fraction part.
	return float64(t.UnixMilli()) / 1000
}
