package logqlmetric

import (
	"math"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql"
)

// SampleOp is a binary operation for two samples.
type SampleOp = func(left, right Sample) (Sample, bool)

func buildSampleBinOp(expr *logql.BinOpExpr) (SampleOp, error) {
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
		return func(left, right Sample) (Sample, bool) {
			result := left
			result.Data += right.Data
			return result, true
		}, nil
	case logql.OpSub:
		return func(left, right Sample) (Sample, bool) {
			result := left
			result.Data -= right.Data
			return result, true
		}, nil
	case logql.OpMul:
		return func(left, right Sample) (Sample, bool) {
			result := left
			result.Data *= right.Data
			return result, true
		}, nil
	case logql.OpDiv:
		return func(left, right Sample) (Sample, bool) {
			result := left
			if right.Data != 0 {
				result.Data /= right.Data
			} else {
				result.Data = math.NaN()
			}
			return result, true
		}, nil
	case logql.OpMod:
		return func(left, right Sample) (Sample, bool) {
			result := left
			if right.Data != 0 {
				result.Data = math.Mod(left.Data, right.Data)
			} else {
				result.Data = math.NaN()
			}
			return result, true
		}, nil
	case logql.OpPow:
		return func(left, right Sample) (Sample, bool) {
			result := left
			result.Data = math.Pow(left.Data, right.Data)
			return result, true
		}, nil
	case logql.OpEq:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data == right.Data, filter)
			return
		}, nil
	case logql.OpNotEq:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data != right.Data, filter)
			return
		}, nil
	case logql.OpGt:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data > right.Data, filter)
			return
		}, nil
	case logql.OpGte:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data >= right.Data, filter)
			return
		}, nil
	case logql.OpLt:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data < right.Data, filter)
			return
		}, nil
	case logql.OpLte:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data <= right.Data, filter)
			return
		}, nil
	default:
		return nil, errors.Errorf("unexpected operation %q", expr.Op)
	}
}
