package logql

import "regexp"

// LogExpr is a log query expression.
//
// See https://grafana.com/docs/loki/latest/logql/log_queries/
type LogExpr struct {
	Sel      Selector
	Pipeline []PipelineStage
}

func (*LogExpr) expr() {}

// Selector is a labels selector.
type Selector struct {
	Matchers []LabelMatcher
}

// LabelMatcher is label matching predicate.
type LabelMatcher struct {
	Label Label
	Op    BinOp          // OpEq, OpNotEq, OpRe, OpNotRe
	Value string         // Equals to value or to unparsed regexp
	Re    *regexp.Regexp // Equals to nil, if Op is not OpRe or OpNotRe
}
