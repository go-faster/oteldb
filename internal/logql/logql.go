// Package logql contains LogQL parser.
package logql

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type LogExpr struct {
	Sel      Selector        `@@`
	Pipeline []PipelineStage `@@*`
}

type Selector struct {
	Matchers []LabelMatcher `"{" ( @@ ( "," @@ )* )? "}"`
}

type LabelMatcher struct {
	Label string         `@Identifier`
	Op    LabelMatcherOp `@( "=~" | "!~" | "=" | "!=" )`
	Value string         `@String`
}

type LabelMatcherOp int

const (
	LabelEq LabelMatcherOp = iota + 1
	LabelNotEq
	LabelRe
	LabelNotRe
)

var matcherOpMap = map[string]LabelMatcherOp{
	"=":  LabelEq,
	"!=": LabelNotEq,
	"=~": LabelRe,
	"!~": LabelNotRe,
}

func (o *LabelMatcherOp) Capture(s []string) error {
	*o = matcherOpMap[s[0]]
	return nil
}

var (
	logqlLexer = lexer.MustSimple([]lexer.SimpleRule{
		{"Keyword", `(json|logfmt|regexp|unpack|pattern|line_format|label_format|decolorize|unwrap|offset|on|ignoring|group_left|group_right|label_replace|ip|distinct|drop|keep|and|or)`},

		{"Bytes", `[0-9,]+(k|m|g|t|p|e)?i?b`},
		{"Duration", `(([0-9])+(n|u|µ|ms|s|m|h|d|w|y))+`},

		{"Identifier", `[a-zA-Z0-9_-]+`},
		{"String", `"[^"]*"`},
		{"Float", `[-+]?(\d*\.)?\d+`},

		{"FilterOp", `==|!=|>=|<=|>|<`},
		{"LabelMatcher", `=~|!~|=|!=`},
		{"LineFilter", `\|=|\|~|!=|!~`},

		{"OpenBrace", `\{`},
		{"CloseBrace", `\}`},
		{"OpenParen", `\(`},
		{"CloseParen", `\)`},
		{"Comma", `,`},
		{"Pipe", `\|`},

		{"whitespace", `\s+`},
	})
	logqlParser = participle.MustBuild[LogExpr](
		participle.Lexer(logqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Duration", "Bytes"),
		participle.UseLookahead(2),
	)
)
