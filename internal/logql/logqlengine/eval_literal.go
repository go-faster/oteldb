package logqlengine

import (
	"math"
	"strconv"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

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

type sampleBinOp = func(left, right sample) (sample, bool)

func buildSampleBinOp(expr *logql.BinOpExpr) (sampleBinOp, error) {
	filter := expr.Modifier.ReturnBool
	boolOp := func(v, filter bool) (float64, bool) {
		if v {
			return 1., true
		}
		// Do not keep the sample, if filter mode is enabled
		return 0., !filter
	}

	switch expr.Op {
	case logql.OpAdd:
		return func(left, right sample) (sample, bool) {
			result := left
			result.data += right.data
			return result, true
		}, nil
	case logql.OpSub:
		return func(left, right sample) (sample, bool) {
			result := left
			result.data -= right.data
			return result, true
		}, nil
	case logql.OpMul:
		return func(left, right sample) (sample, bool) {
			result := left
			result.data *= right.data
			return result, true
		}, nil
	case logql.OpDiv:
		return func(left, right sample) (sample, bool) {
			result := left
			if right.data != 0 {
				result.data /= right.data
			} else {
				result.data = math.NaN()
			}
			return result, true
		}, nil
	case logql.OpMod:
		return func(left, right sample) (sample, bool) {
			result := left
			if right.data != 0 {
				result.data = math.Mod(left.data, right.data)
			} else {
				result.data = math.NaN()
			}
			return result, true
		}, nil
	case logql.OpPow:
		return func(left, right sample) (sample, bool) {
			result := left
			result.data = math.Pow(left.data, right.data)
			return result, true
		}, nil
	case logql.OpEq:
		return func(left, right sample) (result sample, keep bool) {
			result = left
			result.data, keep = boolOp(left.data == right.data, filter)
			return
		}, nil
	case logql.OpNotEq:
		return func(left, right sample) (result sample, keep bool) {
			result = left
			result.data, keep = boolOp(left.data != right.data, filter)
			return
		}, nil
	case logql.OpGt:
		return func(left, right sample) (result sample, keep bool) {
			result = left
			result.data, keep = boolOp(left.data > right.data, filter)
			return
		}, nil
	case logql.OpGte:
		return func(left, right sample) (result sample, keep bool) {
			result = left
			result.data, keep = boolOp(left.data >= right.data, filter)
			return
		}, nil
	case logql.OpLt:
		return func(left, right sample) (result sample, keep bool) {
			result = left
			result.data, keep = boolOp(left.data < right.data, filter)
			return
		}, nil
	case logql.OpLte:
		return func(left, right sample) (result sample, keep bool) {
			result = left
			result.data, keep = boolOp(left.data <= right.data, filter)
			return
		}, nil
	default:
		return nil, errors.Errorf("unexpected operation %s", expr.Op)
	}
}

type literalOpAggIterator struct {
	iter  iterators.Iterator[aggStep]
	op    sampleBinOp
	value float64
	// left whether on which side literal is
	left bool
}

func newLiteralOpAggIterator(
	iter iterators.Iterator[aggStep],
	op sampleBinOp,
	value float64,
	left bool,
) *literalOpAggIterator {
	return &literalOpAggIterator{
		iter:  iter,
		op:    op,
		value: value,
		left:  left,
	}
}

func (i *literalOpAggIterator) Next(r *aggStep) bool {
	if !i.iter.Next(r) {
		return false
	}

	n := 0
	for _, agg := range r.samples {
		literal := sample{
			data: i.value,
			set:  agg.set,
			key:  agg.key,
		}

		var left, right sample
		if i.left {
			left, right = literal, agg
		} else {
			left, right = agg, literal
		}

		if val, ok := i.op(left, right); ok {
			r.samples[n] = val
			n++
		}
	}
	r.samples = r.samples[:n]

	return true
}

func (i *literalOpAggIterator) Err() error {
	return i.iter.Err()
}

func (i *literalOpAggIterator) Close() error {
	return i.iter.Close()
}
