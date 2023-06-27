package logql

type PipelineStage struct {
	LineFilter  *LineFilter             `( @@`
	JSON        *JSONExpressionParser   `| "|" @@`
	Logfmt      *LogfmtExpressionParser `| "|" @@`
	Regexp      *RegexpLabelParser      `| "|" @@`
	Pattern     *PatternLabelParser     `| "|" @@`
	Unpack      bool                    `| "|" @"unpack"`
	LineFormat  *LineFormat             `| "|" @@`
	Decolorize  bool                    `| "|" @"decolorize"`
	LabelFilter *LabelFilter            `| "|" @@ )`
}

type LineFilter struct {
	Op    LineFilterOp `@( "|=" | "|~" | "!=" | "!~" )`
	Value string       `@String`
}

type LineFilterOp int

const (
	LineEq LineFilterOp = iota + 1
	LineNotEq
	LineRe
	LineNotRe
)

var lineFilterOpMap = map[string]LineFilterOp{
	"|=": LineEq,
	"|~": LineRe,
	"!=": LineNotEq,
	"!~": LineNotRe,
}

func (o *LineFilterOp) Capture(s []string) error {
	*o = lineFilterOpMap[s[0]]
	return nil
}

type JSONExpressionParser struct {
	Exprs []LabelExtractionExpr `"json" ( @@ ( "," @@ )* )?`
}

type LogfmtExpressionParser struct {
	Exprs []LabelExtractionExpr `"logfmt" ( @@ ( "," @@ )* )?`
}

type LabelExtractionExpr struct {
	Label string  `( @Identifier`
	EqTo  *string `( "=" @String )? )`
}

type RegexpLabelParser struct {
	Regexp string `"regexp" @String`
}

type PatternLabelParser struct {
	Pattern string `"pattern" @String`
}

type LabelFilter struct {
	Pred  *LabelPredicate  `( @@ `
	Paren *LabelFilter     `| "(" @@ ")" )`
	Next  *NextLabelFilter `@@?`
}

type LabelPredicate struct {
	Matcher  *LabelMatcher   `( @@ `
	Duration *DurationFilter `| @@`
	Bytes    *BytesFilter    `| @@`
	Number   *NumberFilter   `| @@`
	IP       *IPFilter       `| @@ )`
}

type IPFilter struct {
	Label string   `@Identifier`
	Op    FilterOp `@( "==" | "!=" )`
	IP    string   `"ip" "(" @String ")"`
}

type DurationFilter struct {
	Label    string   `@Identifier`
	Op       FilterOp `@( "==" | "!=" | ">=" | "<=" | ">" | "<" )`
	Duration string   `@Duration`
}

type BytesFilter struct {
	Label string   `@Identifier`
	Op    FilterOp `@( "==" | "!=" | ">=" | "<=" | ">" | "<" )`
	Bytes string   `@Bytes`
}

type NumberFilter struct {
	Label  string   `@Identifier`
	Op     FilterOp `@( "==" | "!=" | ">=" | "<=" | ">" | "<" )`
	Number float64  `@Float`
}

type FilterOp int

const (
	FilterOpEq FilterOp = iota + 1
	FilterOpNotEq
	FilterOpGt
	FilterOpGte
	FilterOpLt
	FilterOpLte
)

var filterOpMap = map[string]FilterOp{
	"==": FilterOpEq,
	"!=": FilterOpNotEq,
	">=": FilterOpGte,
	"<=": FilterOpLte,
	">":  FilterOpGt,
	"<":  FilterOpLt,
}

func (o *FilterOp) Capture(s []string) error {
	*o = filterOpMap[s[0]]
	return nil
}

type NextLabelFilter struct {
	Op     string       `@( "," | "or" | "and" )?`
	Filter *LabelFilter `@@`
}

type LineFormat struct {
	Template string `"line_format" @String`
}
