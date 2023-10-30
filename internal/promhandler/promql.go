package promhandler

import (
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/promql"

	"github.com/go-faster/oteldb/internal/promapi"
)

func mapResult(query string, r *promql.Result) (*promapi.QueryResponse, error) {
	if err := r.Err; err != nil {
		return nil, executionErr("query", err)
	}

	resp := &promapi.QueryResponse{
		Status:   "success",
		Warnings: r.Warnings.AsStrings(query, 0),
	}
	switch r := r.Value.(type) {
	case promql.Matrix:
		mat := promapi.Matrix{
			Result: make([]promapi.MatrixResultItem, len(r)),
		}
		for i, e := range r {
			// FIXME(tdakkota): map histogram too.
			values := make([]promapi.Value, len(e.Floats))
			for i, val := range e.Floats {
				values[i] = mapValue(val.T, val.F)
			}
			mat.Result[i] = promapi.MatrixResultItem{
				Metric: e.Metric.Map(),
				Values: values,
			}
		}
		resp.Data.SetMatrix(mat)
	case promql.Vector:
		vec := promapi.Vector{
			Result: make([]promapi.VectorResultItem, len(r)),
		}
		for i, e := range r {
			// FIXME(tdakkota): map histogram too.
			vec.Result[i] = promapi.VectorResultItem{
				Metric: e.Metric.Map(),
				Value:  mapValue(e.T, e.F),
			}
		}
		resp.Data.SetVector(vec)
	case promql.Scalar:
		resp.Data.SetScalar(promapi.Scalar{
			Result: mapValue(r.T, r.V),
		})
	case promql.String:
		resp.Data.SetString(promapi.String{
			Result: promapi.StringValue{
				T: apiTimestamp(r.T),
				V: r.V,
			},
		})
	default:
		return nil, errors.Errorf("unknown value %T", r)
	}

	return resp, nil
}

func apiTimestamp(t int64) float64 {
	return float64(t) / 1000
}

func mapValue(t int64, f float64) promapi.Value {
	return promapi.Value{
		T: apiTimestamp(t),
		V: f,
	}
}
