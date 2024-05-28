package logql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLineFilter_String(t *testing.T) {
	tests := []struct {
		f    LineFilter
		want string
	}{
		{
			LineFilter{
				Op: OpEq,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`|= "line"`,
		},
		{
			LineFilter{
				Op: OpNotEq,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`!= "line"`,
		},
		{
			LineFilter{
				Op: OpRe,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`|~ "line"`,
		},
		{
			LineFilter{
				Op: OpNotRe,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`!~ "line"`,
		},
		{
			LineFilter{
				Op: OpPattern,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`|> "line"`,
		},
		{
			LineFilter{
				Op: OpNotPattern,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`!> "line"`,
		},
		{
			LineFilter{
				Op: OpEq,
				By: LineFilterValue{
					Value: "127.0.0.1",
					IP:    true,
				},
			},
			`|= ip("127.0.0.1")`,
		},
		{
			LineFilter{
				Op: OpEq,
				By: LineFilterValue{
					Value: "foo",
				},
				Or: []LineFilterValue{
					{Value: "bar"},
				},
			},
			`|= "foo" or "bar"`,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.f.String()
			require.Equal(t, tt.want, s)

			// Ensure stringer produces valid LogQL.
			_, err := Parse(`{foo="bar"} `+s, ParseOptions{})
			require.NoError(t, err)
		})
	}
}
