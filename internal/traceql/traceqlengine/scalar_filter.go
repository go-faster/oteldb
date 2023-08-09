package traceqlengine

import (
	"fmt"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/traceql"
)

// ScalarFilter filters Scalars by field expression.
type ScalarFilter struct {
	left  aggregator
	op    binaryOp
	right traceql.Static
}

func buildScalarFilter(filter *traceql.ScalarFilter) (Processor, error) {
	left, ok := filter.Left.(*traceql.AggregateScalarExpr)
	if !ok {
		return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported left scalar expression %T", filter.Left)}
	}

	static, ok := filter.Right.(*traceql.Static)
	if !ok {
		return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported right scalar expression %T", filter.Right)}
	}

	agg, err := buildAggregator(left)
	if err != nil {
		return nil, errors.Wrap(err, "build aggregator")
	}

	op, err := buildBinaryOp(filter.Op, "")
	if err != nil {
		return nil, err
	}

	return &ScalarFilter{
		left:  agg,
		op:    op,
		right: *static,
	}, nil
}

// Process implements Processor.
func (f *ScalarFilter) Process(sets []Spanset) (result []Spanset, _ error) {
	for _, set := range sets {
		if f.keep(set) {
			result = append(result, set)
		}
	}
	return result, nil
}

func (f *ScalarFilter) keep(set Spanset) bool {
	var left traceql.Static
	left.SetNumber(f.left.Aggregate(set))

	result := f.op(left, f.right)
	return result.Type == traceql.TypeBool && result.AsBool()
}

type aggregator interface {
	Aggregate(set Spanset) float64
}

func buildAggregator(expr *traceql.AggregateScalarExpr) (_ aggregator, err error) {
	var eval evaluater
	if expr.Op != traceql.AggregateOpCount {
		eval, err = buildEvaluater(expr.Field)
		if err != nil {
			return nil, errors.Wrap(err, "build aggregate evaluater")
		}
	}

	switch expr.Op {
	case traceql.AggregateOpCount:
		return &countAgg{}, nil
	case traceql.AggregateOpMax:
		return &maxAgg{eval: eval}, nil
	case traceql.AggregateOpMin:
		return &minAgg{eval: eval}, nil
	case traceql.AggregateOpAvg:
		return &avgAgg{sum: sumAgg{eval: eval}}, nil
	case traceql.AggregateOpSum:
		return &sumAgg{eval: eval}, nil
	default:
		return nil, errors.Errorf("unexpected aggregate op %q", expr.Op)
	}
}

type countAgg struct{}

func (*countAgg) Aggregate(set Spanset) float64 {
	return float64(len(set.Spans))
}

type maxAgg struct {
	eval evaluater
}

func (agg *maxAgg) Aggregate(set Spanset) (r float64) {
	var (
		ectx      = set.evaluateCtx()
		resultSet bool
	)
	for _, span := range set.Spans {
		result := agg.eval(span, ectx)
		if !result.Type.IsNumeric() {
			continue
		}
		if val := result.AsFloat(); !resultSet || val > r {
			r = val
			resultSet = true
		}
	}
	return r
}

type minAgg struct {
	eval evaluater
}

func (agg *minAgg) Aggregate(set Spanset) (r float64) {
	var (
		ectx      = set.evaluateCtx()
		resultSet bool
	)
	for _, span := range set.Spans {
		result := agg.eval(span, ectx)
		if !result.Type.IsNumeric() {
			continue
		}
		if val := result.AsFloat(); !resultSet || val < r {
			r = val
			resultSet = true
		}
	}
	return r
}

type avgAgg struct {
	sum sumAgg
}

func (agg *avgAgg) Aggregate(set Spanset) float64 {
	count := len(set.Spans)
	if count == 0 {
		return 0
	}

	sum := agg.sum.Aggregate(set)
	return sum / float64(count)
}

type sumAgg struct {
	eval evaluater
}

func (agg *sumAgg) Aggregate(set Spanset) (r float64) {
	var (
		ectx = set.evaluateCtx()
		sum  float64
	)
	for _, span := range set.Spans {
		result := agg.eval(span, ectx)
		if !result.Type.IsNumeric() {
			continue
		}
		sum += result.AsFloat()
	}
	return sum
}
