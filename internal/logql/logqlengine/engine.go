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
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
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

	iter, err := e.buildAggStepIterator(ctx, expr, params)
	if err != nil {
		return data, err
	}
	defer func() {
		_ = iter.Close()
	}()
	return readStepResponse(iter, params.IsInstant())
}

func (e *Engine) buildAggStepIterator(ctx context.Context, expr logql.Expr, params EvalParams) (iterators.Iterator[aggStep], error) {
	switch expr := logql.UnparenExpr(expr).(type) {
	case *logql.BinOpExpr:
		if lit, ok := expr.Left.(*logql.LiteralExpr); ok {
			op, err := buildSampleBinOp(expr)
			if err != nil {
				return nil, err
			}

			right, err := e.buildAggStepIterator(ctx, expr.Right, params)
			if err != nil {
				return nil, err
			}
			return newLiteralOpAggIterator(right, op, lit.Value, true), nil
		}
		if lit, ok := expr.Right.(*logql.LiteralExpr); ok {
			op, err := buildSampleBinOp(expr)
			if err != nil {
				return nil, err
			}

			left, err := e.buildAggStepIterator(ctx, expr.Left, params)
			if err != nil {
				return nil, err
			}
			return newLiteralOpAggIterator(left, op, lit.Value, false), nil
		}
	case *logql.RangeAggregationExpr:
		return e.rangeAggIterator(ctx, expr, params)
	}

	return nil, &UnsupportedError{Msg: fmt.Sprintf("expression %T is not supported yet", expr)}
}

func readStepResponse(iter iterators.Iterator[aggStep], instant bool) (s lokiapi.QueryResponseData, _ error) {
	var (
		agg          aggStep
		matrixSeries map[string]lokiapi.Series
	)
	for {
		if !iter.Next(&agg) {
			break
		}

		if instant {
			if err := iter.Err(); err != nil {
				return s, err
			}

			var vector lokiapi.Vector
			for _, s := range agg.samples {
				vector = append(vector, lokiapi.Sample{
					Metric: lokiapi.NewOptLabelSet(s.set),
					Value: lokiapi.FPoint{
						T: getPrometheusTimestamp(agg.ts.AsTime()),
						V: strconv.FormatFloat(s.data, 'f', -1, 64),
					},
				})
			}

			s.SetVectorResult(lokiapi.VectorResult{
				Result: vector,
			})
			return s, nil
		}

		if matrixSeries == nil {
			matrixSeries = map[string]lokiapi.Series{}
		}
		for _, s := range agg.samples {
			ser, ok := matrixSeries[s.key]
			if !ok {
				ser.Metric.SetTo(s.set)
			}

			ser.Values = append(ser.Values, lokiapi.FPoint{
				T: getPrometheusTimestamp(agg.ts.AsTime()),
				V: strconv.FormatFloat(s.data, 'f', -1, 64),
			})
			matrixSeries[s.key] = ser
		}
	}
	if err := iter.Err(); err != nil {
		return s, err
	}

	// Sort points inside series.
	for k, s := range matrixSeries {
		slices.SortFunc(s.Values, func(a, b lokiapi.FPoint) bool {
			return a.T < b.T
		})
		matrixSeries[k] = s
	}
	result := maps.Values(matrixSeries)
	slices.SortFunc(result, func(a, b lokiapi.Series) bool {
		if len(a.Values) < 1 || len(b.Values) < 1 {
			return len(a.Values) < len(b.Values)
		}
		return a.Values[0].T < b.Values[0].T
	})

	s.SetMatrixResult(lokiapi.MatrixResult{
		Result: result,
	})
	return s, iter.Err()
}

func addDuration(ts otelstorage.Timestamp, d time.Duration) otelstorage.Timestamp {
	return otelstorage.NewTimestampFromTime(ts.AsTime().Add(d))
}
