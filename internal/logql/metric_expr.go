package logql

import (
	"regexp"

	"github.com/go-faster/errors"
)

// MetricExpr is a metric query expression.
//
// See https://grafana.com/docs/loki/latest/logql/metric_queries/.
type MetricExpr interface {
	Expr
	metricExpr()
}

func (*RangeAggregationExpr) expr()  {}
func (*VectorAggregationExpr) expr() {}
func (*LiteralExpr) expr()           {}
func (*LabelReplaceExpr) expr()      {}
func (*VectorExpr) expr()            {}

func (*RangeAggregationExpr) metricExpr()  {}
func (*VectorAggregationExpr) metricExpr() {}
func (*LiteralExpr) metricExpr()           {}
func (*LabelReplaceExpr) metricExpr()      {}
func (*VectorExpr) metricExpr()            {}

// RangeAggregationExpr is a range aggregation expression.
type RangeAggregationExpr struct {
	Op        RangeOp
	Range     LogRangeExpr
	Parameter *float64
	Grouping  *Grouping
}

func (e *RangeAggregationExpr) validate() error {
	switch {
	case e.Parameter != nil && e.Op != RangeOpQuantile:
		return errors.Errorf("parameter is not supported for operation %q", e.Op)
	case e.Parameter == nil && e.Op == RangeOpQuantile:
		return errors.Errorf("parameter is required for operation %q", e.Op)
	}

	if e.Grouping != nil {
		switch e.Op {
		case RangeOpAvg,
			RangeOpStddev,
			RangeOpStdvar,
			RangeOpQuantile,
			RangeOpMax,
			RangeOpMin,
			RangeOpFirst,
			RangeOpLast:
		default:
			return errors.Errorf("grouping aggregation is not allowed for operation %q", e.Op)
		}
	}

	if e.Range.Unwrap != nil {
		switch e.Op {
		case RangeOpAvg,
			RangeOpSum,
			RangeOpMax,
			RangeOpMin,
			RangeOpStddev,
			RangeOpStdvar,
			RangeOpQuantile,
			RangeOpRate,
			RangeOpRateCounter,
			RangeOpAbsent,
			RangeOpFirst,
			RangeOpLast:
		default:
			return errors.Errorf("unwrap aggregation is not allowed for operation %q", e.Op)
		}
	} else {
		switch e.Op {
		case RangeOpBytes,
			RangeOpBytesRate,
			RangeOpCount,
			RangeOpRate,
			RangeOpAbsent:
		default:
			return errors.Errorf("unwrap aggregation is required for operation %q", e.Op)
		}
	}
	return nil
}

// VectorAggregationExpr is a vector aggregation expression.
type VectorAggregationExpr struct {
	Op        VectorOp
	Expr      MetricExpr
	Parameter *int
	Grouping  *Grouping
}

func (e *VectorAggregationExpr) validate() error {
	switch e.Op {
	case VectorOpTopk, VectorOpBottomk:
		if e.Parameter == nil {
			return errors.Errorf("parameter is required for operation %q", e.Op)
		}
		if param := *e.Parameter; param <= 0 {
			return errors.Errorf("parameter must be greater than 0, got %d", param)
		}
	default:
		if e.Parameter != nil {
			return errors.Errorf("parameter is not supported for operation %q", e.Op)
		}
	}

	switch e.Op {
	case VectorOpSort, VectorOpSortDesc:
		if e.Grouping != nil {
			return errors.Errorf("grouping is not supported for operation %q", e.Op)
		}
	}
	return nil
}

// LiteralExpr is a literal expression.
type LiteralExpr struct {
	Value float64
}

// LabelReplaceExpr is a PromQL `label_replace` function.
type LabelReplaceExpr struct {
	Expr        MetricExpr
	DstLabel    string
	Replacement string
	SrcLabel    string
	Regex       string
	Re          *regexp.Regexp // Compiled Regex
}

// VectorExpr is a vector expression.
type VectorExpr struct {
	Value float64
}

// Grouping is a grouping clause.
type Grouping struct {
	Labels  []Label
	Without bool
}
