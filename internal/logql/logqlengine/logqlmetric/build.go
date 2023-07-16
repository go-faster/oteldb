package logqlmetric

import (
	"fmt"
	"io"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
)

// SampleSelector creates new sampled entry iterator.
type SampleSelector = func(sel logql.LogRangeExpr) (iterators.Iterator[SampledEntry], error)

// EvalParams is a query evaluation params.
type EvalParams struct {
	Start, End time.Time
	Step       time.Duration
}

// Build builds new step iterator.
func Build(expr logql.MetricExpr, sel SampleSelector, params EvalParams) (StepIterator, error) {
	return build(expr, sel, params)
}

func build(expr logql.Expr, sel SampleSelector, params EvalParams) (_ StepIterator, rerr error) {
	closeOnError := func(c io.Closer) {
		if rerr != nil {
			_ = c.Close()
		}
	}

	switch expr := logql.UnparenExpr(expr).(type) {
	case *logql.RangeAggregationExpr:
		iter, err := sel(expr.Range)
		if err != nil {
			return nil, errors.Wrap(err, "get samples iterator")
		}
		defer closeOnError(iter)

		return RangeAggregation(iter, expr, params.Start, params.End, params.Step)
	case *logql.VectorAggregationExpr:
		iter, err := build(expr.Expr, sel, params)
		if err != nil {
			return nil, err
		}
		defer closeOnError(iter)

		return VectorAggregation(iter, expr)
	case *logql.LiteralExpr:
	case *logql.LabelReplaceExpr:
	case *logql.VectorExpr:
		return Vector(expr, params.Start, params.End, params.Step), nil
	case *logql.BinOpExpr:
		if lit, ok := expr.Left.(*logql.LiteralExpr); ok {
			right, err := build(expr.Right, sel, params)
			if err != nil {
				return nil, err
			}
			return LiteralBinOp(right, expr, lit.Value, true)
		}
		if lit, ok := expr.Right.(*logql.LiteralExpr); ok {
			left, err := build(expr.Left, sel, params)
			if err != nil {
				return nil, err
			}
			return LiteralBinOp(left, expr, lit.Value, false)
		}

		left, err := build(expr.Left, sel, params)
		if err != nil {
			return nil, err
		}
		defer closeOnError(left)

		right, err := build(expr.Right, sel, params)
		if err != nil {
			return nil, err
		}
		defer closeOnError(right)

		return BinOp(left, right, expr)
	default:
		return nil, errors.Errorf("unexpected expression %T", expr)
	}
	return nil, &UnsupportedError{Msg: fmt.Sprintf("expression %T is not supported yet", expr)}
}
