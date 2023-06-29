package logql

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

// VectorAggregationExpr is a vector aggregation expression.
type VectorAggregationExpr struct {
	Op        VectorOp
	Expr      MetricExpr
	Parameter *float64
	Grouping  *Grouping
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
}

// VectorExpr is a vector expression.
type VectorExpr struct {
	Value float64
}

// Grouping is a grouping clause.
type Grouping struct {
	Op     GroupingOp
	Labels []Label
}
