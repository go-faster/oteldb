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
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Engine is a LogQL evaluation engine.
type Engine struct {
	querier     Querier
	querierCaps QuerierСapabilities

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
		querierCaps:      querier.Сapabilities(),
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
	Direction string // forward, backward
	Limit     int
}

// IsInstant whether query is instant.
func (p EvalParams) IsInstant() bool {
	return p.Start == p.End && p.Step == 0
}

// Eval parses and evaluates query.
func (e *Engine) Eval(ctx context.Context, query string, params EvalParams) (data lokiapi.QueryResponseData, rerr error) {
	ctx, span := e.tracer.Start(ctx, "Eval",
		trace.WithAttributes(
			attribute.String("logql.query", query),
			attribute.Int64("logql.start", int64(params.Start)),
			attribute.Int64("logql.end", int64(params.End)),
			attribute.Int64("logql.step", int64(params.Step)),
			attribute.String("logql.direction", params.Direction),
			attribute.Int("logql.limit", params.Limit),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
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
	switch expr := logql.UnparenExpr(expr).(type) {
	case *logql.LogExpr:
		streams, err := e.evalLogExpr(ctx, expr, params)
		if err != nil {
			return data, err
		}

		data.SetStreamsResult(lokiapi.StreamsResult{
			Result: streams,
		})
		return data, nil
	case *logql.LiteralExpr:
		return e.evalLiteral(expr, params), nil
	case *logql.VectorExpr:
		return e.evalVector(expr, params), nil
	case *logql.BinOpExpr:
		reduced, err := logql.ReduceBinOp(expr)
		if err != nil {
			return data, errors.Wrap(err, "reduce binop")
		}
		if reduced != nil {
			return e.evalLiteral(reduced, params), nil
		}
	}

	return data, &UnsupportedError{Msg: fmt.Sprintf("expression %T is not supported yet", expr)}
}

func (e *Engine) evalLogExpr(ctx context.Context, expr *logql.LogExpr, params EvalParams) (s lokiapi.Streams, _ error) {
	// Instant query, sub lookback duration from Start.
	if params.IsInstant() {
		newStart := params.Start.AsTime().Add(e.lookbackDuration)
		params.Start = otelstorage.NewTimestampFromTime(newStart)
	}

	cond, err := extractQueryConditions(e.querierCaps, expr.Sel)
	if err != nil {
		return s, errors.Wrap(err, "extract preconditions")
	}

	pipeline, err := BuildPipeline(expr.Pipeline...)
	if err != nil {
		return s, errors.Wrap(err, "build pipeline")
	}

	iter, err := e.querier.SelectLogs(ctx,
		params.Start,
		params.End,
		cond.params,
	)
	if err != nil {
		return s, errors.Wrap(err, "query")
	}
	defer func() {
		_ = iter.Close()
	}()

	var (
		lg = zctx.From(ctx)

		record logstorage.Record
		set    = newLabelSet()

		streams = map[string]lokiapi.Stream{}
		entries int
	)
	for iter.Next(&record) {
		if entries >= params.Limit {
			break
		}

		if err := set.SetFromRecord(record); err != nil {
			lg.Warn("Invalid log record", zap.Uint64("ts", uint64(record.Timestamp)), zap.Error(err))
			// Just skip the line.
			continue
		}

		line, keep := cond.prefilter.Process(record.Timestamp, record.Body, set)
		if !keep {
			continue
		}

		line, keep = pipeline.Process(record.Timestamp, line, set)
		if !keep {
			continue
		}

		// FIXME(tdakkota): allocates a string for every record.
		key := set.String()
		stream, ok := streams[key]
		if !ok {
			stream = lokiapi.Stream{
				Stream: lokiapi.NewOptLabelSet(set.AsLokiAPI()),
			}
		}

		entries++
		stream.Values = append(stream.Values, lokiapi.LogEntry{T: uint64(record.Timestamp), V: line})

		streams[key] = stream
	}
	lg.Debug("Eval completed", zap.Int("entries", entries))

	return maps.Values(streams), nil
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

func (e *Engine) evalVector(expr *logql.VectorExpr, params EvalParams) (data lokiapi.QueryResponseData) {
	if params.IsInstant() {
		pair := lokiapi.FPoint{
			T: getPrometheusTimestamp(params.Start.AsTime()),
			V: strconv.FormatFloat(expr.Value, 'f', -1, 64),
		}
		data.SetVectorResult(lokiapi.VectorResult{
			Result: lokiapi.Vector{
				{Value: pair},
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

		series lokiapi.Series
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
