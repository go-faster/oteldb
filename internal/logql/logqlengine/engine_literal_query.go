package logqlengine

import (
	"context"
	"strconv"
	"time"

	"github.com/go-faster/oteldb/internal/lokiapi"
)

// LiteralQuery is simple literal expression query.
type LiteralQuery struct {
	Value float64
}

var _ Query = (*LiteralQuery)(nil)

// Eval implements [Query].
func (q *LiteralQuery) Eval(ctx context.Context, params EvalParams) (data lokiapi.QueryResponseData, _ error) {
	if params.IsInstant() {
		data.SetScalarResult(lokiapi.ScalarResult{
			Result: lokiapi.FPoint{
				T: getPrometheusTimestamp(params.Start),
				V: strconv.FormatFloat(q.Value, 'f', -1, 64),
			},
		})
		return data, nil
	}
	data.SetMatrixResult(lokiapi.MatrixResult{
		Result: generateLiteralMatrix(q.Value, params),
	})
	return data, nil
}

func generateLiteralMatrix(value float64, params EvalParams) lokiapi.Matrix {
	var (
		start = params.Start
		end   = params.End

		series = lokiapi.Series{
			Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet{}),
		}
	)

	var (
		until    = end.Add(params.Step)
		strValue = strconv.FormatFloat(value, 'f', -1, 64)
	)
	for ts := start; !ts.After(until); ts = ts.Add(params.Step) {
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
