package logqlengine

import (
	"strconv"
	"time"

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
