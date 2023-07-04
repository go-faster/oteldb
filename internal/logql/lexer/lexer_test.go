package lexer

import (
	"fmt"
	"testing"
	"text/scanner"

	"github.com/stretchr/testify/require"
)

type TestCase struct {
	input   string
	want    []Token
	wantErr bool
}

var tests = []TestCase{
	{
		`3h`,
		[]Token{
			{Type: Duration, Text: "3h"},
		},
		false,
	},
	{
		`3h2m1.99s`,
		[]Token{
			{Type: Duration, Text: "3h2m1.99s"},
		},
		false,
	},
	{
		`32kb`,
		[]Token{
			{Type: Bytes, Text: "32kb"},
		},
		false,
	},
	{
		`32.4kb`,
		[]Token{
			{Type: Bytes, Text: "32.4kb"},
		},
		false,
	},
	{
		`{}`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: CloseBrace, Text: "}"},
		},
		false,
	},
	{
		`{foo =~ "bar"}`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: "foo"},
			{Type: Re, Text: "=~"},
			{Type: String, Text: "bar"},
			{Type: CloseBrace, Text: "}"},
		},
		false,
	},
	{
		`{http.method =~ "(GET|POST)"}`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: "http.method"},
			{Type: Re, Text: "=~"},
			{Type: String, Text: "(GET|POST)"},
			{Type: CloseBrace, Text: "}"},
		},
		false,
	},
	{
		`{duration =~ "bar"} | duration > 10 and duration(duration) > 10`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: "duration"},
			{Type: Re, Text: "=~"},
			{Type: String, Text: "bar"},
			{Type: CloseBrace, Text: "}"},

			{Type: Pipe, Text: "|"},
			// duration > 10
			{Type: Ident, Text: "duration"},
			{Type: Gt, Text: ">"},
			{Type: Number, Text: "10"},
			// and
			{Type: And, Text: "and"},
			// duration(duration) > 10
			{Type: DurationConv, Text: "duration"},
			{Type: OpenParen, Text: "("},
			{Type: Ident, Text: "duration"},
			{Type: CloseParen, Text: ")"},
			{Type: Gt, Text: ">"},
			{Type: Number, Text: "10"},
		},
		false,
	},
	{
		`{ip =~ "1"} | size > 20kb`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: "ip"},
			{Type: Re, Text: "=~"},
			{Type: String, Text: "1"},
			{Type: CloseBrace, Text: "}"},

			{Type: Pipe, Text: "|"},
			{Type: Ident, Text: "size"},
			{Type: Gt, Text: ">"},
			{Type: Bytes, Text: "20kb"},
		},
		false,
	},
	{
		`{name="kafka" , label=~"sus"}
		|= "bad"
		| json
		| json foo, bar
		| json foo="10", bar="sus"
		| logfmt foo="10", bar="sus"`,
		[]Token{
			{Type: OpenBrace, Text: "{"},
			{Type: Ident, Text: "name"},
			{Type: Eq, Text: "="},
			{Type: String, Text: "kafka"},
			{Type: Comma, Text: ","},
			{Type: Ident, Text: "label"},
			{Type: Re, Text: "=~"},
			{Type: String, Text: "sus"},
			{Type: CloseBrace, Text: "}"},

			{Type: PipeExact, Text: "|="},
			{Type: String, Text: "bad"},

			{Type: Pipe, Text: "|"},
			{Type: JSON, Text: "json"},

			{Type: Pipe, Text: "|"},
			{Type: JSON, Text: "json"},
			{Type: Ident, Text: "foo"},
			{Type: Comma, Text: ","},
			{Type: Ident, Text: "bar"},

			{Type: Pipe, Text: "|"},
			{Type: JSON, Text: "json"},
			{Type: Ident, Text: "foo"},
			{Type: Eq, Text: "="},
			{Type: String, Text: "10"},
			{Type: Comma, Text: ","},
			{Type: Ident, Text: "bar"},
			{Type: Eq, Text: "="},
			{Type: String, Text: "sus"},

			{Type: Pipe, Text: "|"},
			{Type: Logfmt, Text: "logfmt"},
			{Type: Ident, Text: "foo"},
			{Type: Eq, Text: "="},
			{Type: String, Text: "10"},
			{Type: Comma, Text: ","},
			{Type: Ident, Text: "bar"},
			{Type: Eq, Text: "="},
			{Type: String, Text: "sus"},
		},
		false,
	},
	{
		`sum ({})`,
		[]Token{
			{Type: Sum, Text: "sum"},
			{Type: OpenParen, Text: "("},
			{Type: OpenBrace, Text: "{"},
			{Type: CloseBrace, Text: "}"},
			{Type: CloseParen, Text: ")"},
		},
		false,
	},
}

func TestTokenize(t *testing.T) {
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := Tokenize(tt.input, TokenizeOptions{AllowDots: true})
			if tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.NoError(t, err)

			for i := range got {
				// Zero position before checking.
				got[i].Pos = scanner.Position{}
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func FuzzTokenize(f *testing.F) {
	for _, tt := range tests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if r := recover(); r != nil || t.Failed() {
				t.Logf("Input:\n%s", input)
			}

			_, _ = Tokenize(input, TokenizeOptions{AllowDots: true})
		}()
	})
}
