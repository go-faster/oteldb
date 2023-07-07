package logql

import (
	"regexp"
	"time"
)

// PipelineStage is a LogQL pipeline stage.
type PipelineStage interface {
	pipelineStage()
}

func (*LineFilter) pipelineStage()             {}
func (*JSONExpressionParser) pipelineStage()   {}
func (*LogfmtExpressionParser) pipelineStage() {}
func (*RegexpLabelParser) pipelineStage()      {}
func (*PatternLabelParser) pipelineStage()     {}
func (*UnpackLabelParser) pipelineStage()      {}
func (*LineFormat) pipelineStage()             {}
func (*DecolorizeExpr) pipelineStage()         {}
func (*LabelFilter) pipelineStage()            {}
func (*LabelFormatExpr) pipelineStage()        {}
func (*DropLabelsExpr) pipelineStage()         {}
func (*KeepLabelsExpr) pipelineStage()         {}
func (*DistinctFilter) pipelineStage()         {}

// LineFilter is a line filter (`|=`, `!=`, `=~`, `!~`).
type LineFilter struct {
	Op    BinOp          // OpEq, OpNotEq, OpRe, OpNotRe
	Value string         // Equals to value or to unparsed regexp
	Re    *regexp.Regexp // Equals to nil, if Op is not OpRe or OpNotRe
	IP    bool           // true, if this line filter is IP filter.
}

// JSONExpressionParser extracts and filters labels from JSON.
type JSONExpressionParser struct {
	// Labels is a set of labels to extract.
	Labels []Label
	// Exprs is a set of extraction expressions.
	Exprs []LabelExtractionExpr
}

// LogfmtExpressionParser extracts and filters labels from Logfmt.
type LogfmtExpressionParser struct {
	// Labels is a set of labels to extract.
	Labels []Label
	// Exprs is a set of extraction expressions.
	Exprs []LabelExtractionExpr
}

// LabelExtractionExpr defines label value to extract.
type LabelExtractionExpr struct {
	Label Label
	Expr  string
}

// RegexpLabelParser extracts labels using regexp capture groups.
type RegexpLabelParser struct {
	Regexp  *regexp.Regexp
	Mapping map[int]Label
}

// PatternLabelParser extracts labels using log pattern.
//
// See https://grafana.com/docs/loki/latest/logql/log_queries/#pattern.
type PatternLabelParser struct {
	Pattern string
}

// UnpackLabelParser unpacks data from promtail.
//
// See https://grafana.com/docs/loki/latest/logql/log_queries/#unpack.
type UnpackLabelParser struct{}

// LineFormat formats log record using Go template.
type LineFormat struct {
	Template string
}

// DecolorizeExpr decolorizes log line.
type DecolorizeExpr struct{}

// LabelFilter filters records by predicate.
type LabelFilter struct {
	Pred LabelPredicate
}

// LabelPredicate is a label predicate.
type LabelPredicate interface {
	labelPredicate()
}

// LabelPredicateBinOp defines a logical operation between predicates.
type LabelPredicateBinOp struct {
	Left  LabelPredicate
	Op    BinOp // OpAnd, OpOr
	Right LabelPredicate
}

// LabelPredicateParen is a prediacte within parenthesis.
//
// FIXME(tdakkota): are we really need it?
type LabelPredicateParen struct {
	X LabelPredicate
}

func (*LabelPredicateBinOp) labelPredicate() {}
func (*LabelPredicateParen) labelPredicate() {}
func (*LabelMatcher) labelPredicate()        {}
func (*DurationFilter) labelPredicate()      {}
func (*BytesFilter) labelPredicate()         {}
func (*NumberFilter) labelPredicate()        {}
func (*IPFilter) labelPredicate()            {}

// IPFilter is a IP filtering predicate (`addr == ip("127.0.0.1")`).
type IPFilter struct {
	Label Label
	Op    BinOp // OpEq, OpNotEq
	Value string
}

// DurationFilter is a duration filtering predicate (`elapsed > 10s`).
type DurationFilter struct {
	Label Label
	Op    BinOp // OpEq, OpNotEq, OpLt, OpLte, OpGt, OpGte
	Value time.Duration
}

// BytesFilter is a byte size filtering predicate (`size > 10gb`).
type BytesFilter struct {
	Label Label
	Op    BinOp // OpEq, OpNotEq, OpLt, OpLte, OpGt, OpGte
	Value uint64
}

// NumberFilter is a number filtering predicate (`status >= 400`).
type NumberFilter struct {
	Label Label
	Op    BinOp // OpEq, OpNotEq, OpLt, OpLte, OpGt, OpGte
	// FIXME(tdakkota): add integer field?
	Value float64
}

// LabelFormatExpr renames, modifies or add labels.
type LabelFormatExpr struct {
	// FIXME(tdakkota): use map[K][]V?
	Labels []RenameLabel
	Values []LabelTemplate
}

// LabelTemplate sets value for a label.
type LabelTemplate struct {
	Label    Label
	Template string
}

// RenameLabel renames label.
type RenameLabel struct {
	Label Label
	To    Label
}

// DropLabelsExpr drops given labels in a pipeline (i.e. deny list).
type DropLabelsExpr struct {
	Labels   []Label
	Matchers []LabelMatcher
}

// KeepLabelsExpr drops any label except given in a pipeline (i.e. allow list).
type KeepLabelsExpr struct {
	Labels   []Label
	Matchers []LabelMatcher
}

// DistinctFilter filters out lines with duplicate label values.
//
// FIXME(tdakkota): this stage is undocumented.
type DistinctFilter struct {
	Labels []Label
}
