package logqlpattern

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var parseTests = []struct {
	input   string
	wantP   []Part
	wantErr string
}{
	{
		"<foo>",
		[]Part{capture("foo")},
		"",
	},
	{
		"<_>",
		[]Part{capture("_")},
		"",
	},
	{
		"<_1foo>",
		[]Part{capture("_1foo")},
		"",
	},
	{
		"<1> <foo>",
		[]Part{
			literal("<1> "),
			capture("foo"),
		},
		"",
	},
	{
		"<foo ><foo>",
		[]Part{
			literal("<foo >"),
			capture("foo"),
		},
		"",
	},
	{
		"<foo>|<bar>",
		[]Part{
			capture("foo"),
			literal("|"),
			capture("bar"),
		},
		"",
	},
	{
		"> <method> <",
		[]Part{
			literal("> "),
			capture("method"),
			literal(" <"),
		},
		"",
	},
	{
		"_> <method> <_",
		[]Part{
			literal("_> "),
			capture("method"),
			literal(" "),
			literal("<_"),
		},
		"",
	},
	{
		"status:<status>",
		[]Part{
			literal("status:"),
			capture("status"),
		},
		"",
	},
	{
		"<method> - status:<status>",
		[]Part{
			capture("method"),
			literal(" - status:"),
			capture("status"),
		},
		"",
	},
	{
		`<ip> - <user> [<_>] "<method> <path> <_>" <status> <size> <url> <user_agent>`,
		[]Part{
			capture("ip"),
			literal(" - "),
			capture("user"),
			literal(" ["),
			capture("_"),
			literal(`] "`),
			capture("method"),
			literal(" "),
			capture("path"),
			literal(" "),
			capture("_"),
			literal(`" `),
			capture("status"),
			literal(" "),
			capture("size"),
			literal(" "),
			capture("url"),
			literal(" "),
			capture("user_agent"),
		},
		"",
	},

	{
		"",
		nil,
		"pattern is empty",
	},
	{
		" ",
		nil,
		"at least one capture is expected",
	},
	{
		"status:",
		nil,
		"at least one capture is expected",
	},
	{
		"<foo>|<foo>",
		nil,
		`duplicate capture "foo"`,
	},
	{
		"<foo><bar>",
		nil,
		"consecutive capture: literal expected between <foo> and <bar>",
	},
}

func capture(s string) Part {
	return Part{
		Type:  Capture,
		Value: s,
	}
}

func literal(s string) Part {
	return Part{
		Type:  Literal,
		Value: s,
	}
}

func TestParse(t *testing.T) {
	for i, tt := range parseTests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if r := recover(); t.Failed() || r != nil {
					t.Logf("Input: %q", tt.input)
				}
			}()

			gotP, err := Parse(tt.input)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantP, gotP.Parts)
		})
	}
}

func FuzzParse(f *testing.F) {
	for _, tt := range parseTests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if r := recover(); t.Failed() || r != nil {
				t.Logf("Input: %q", input)
			}
		}()

		Parse(input)
	})
}
