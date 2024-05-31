package logql

import (
	"fmt"
	"regexp"
	"strings"
)

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

// String implements [fmt.Stringer].
func (s Selector) String() string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, m := range s.Matchers {
		if i != 0 {
			sb.WriteByte(',')
		}
		// FIXME(tdakkota): suboptimal
		sb.WriteString(m.String())
	}
	sb.WriteByte('}')
	return sb.String()
}

// LabelMatcher is label matching predicate.
type LabelMatcher struct {
	Label Label
	Op    BinOp          // OpEq, OpNotEq, OpRe, OpNotRe
	Value string         // Equals to value or to unparsed regexp
	Re    *regexp.Regexp // Equals to nil, if Op is not OpRe or OpNotRe
}

// String implements [fmt.Stringer].
func (m LabelMatcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Label, m.Op, m.Value)
}
