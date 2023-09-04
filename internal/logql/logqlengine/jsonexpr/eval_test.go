package jsonexpr

import (
	"fmt"
	"testing"

	"github.com/go-faster/jx"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/logql"
)

func TestExtract(t *testing.T) {
	parseExprs := func(exprs ...string) map[logql.Label]Path {
		selectors := make(map[logql.Label]Path, len(exprs))
		for _, expr := range exprs {
			sel, err := Parse(expr)
			require.NoError(t, err)
			selectors[logql.Label(expr)] = sel
		}
		return selectors
	}

	tests := []struct {
		input   string
		paths   map[logql.Label]Path
		want    map[logql.Label]string
		wantErr bool
	}{
		{
			`{
				"literal": "foo",
				"obj": {"c": "bar"},
				"arr": [{"e": "baz"}]
			}`,
			parseExprs(
				"literal",
				"obj",
				"obj.c",
				`obj["c"]`,
				"arr",
				"arr[0]",
				"arr[0].e",

				// Wrong type.
				"literal[0]",
				"literal.key",
				`literal["key"]`,
				"obj[0]",
				"arr.key",
			),
			map[logql.Label]string{
				"literal":  "foo",
				"obj":      `{"c": "bar"}`,
				"obj.c":    "bar",
				`obj["c"]`: "bar",
				"arr":      `[{"e": "baz"}]`,
				"arr[0]":   `{"e": "baz"}`,
				"arr[0].e": "baz",
			},
			false,
		},
		// Test different types.
		{
			`{
				"str": "foo\n",
				"integer": 10,
				"float": 3.14,
				"null": null,
				"bool": true
			}`,
			parseExprs(
				"str",
				"integer",
				"float",
				"null",
				"bool",
			),
			map[logql.Label]string{
				"str":     "foo\n",
				"integer": "10",
				"float":   "3.14",
				"null":    "",
				"bool":    "true",
			},
			false,
		},

		// Invalid JSON.
		{
			`{`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": }`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": {,}}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": [,]}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": "foo\"}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`["foo\"]`,
			map[logql.Label]Path{"a": {IndexSel(0)}},
			nil,
			true,
		},
		{
			`{"a": 1ee1}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": nul}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
		{
			`{"a": tru}`,
			map[logql.Label]Path{"a": {KeySel("a")}},
			nil,
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			d := jx.DecodeStr(tt.input)
			got := make(map[logql.Label]string, len(tt.paths))

			err := Extract(
				d,
				tt.paths,
				func(l logql.Label, s string) {
					got[l] = s
				},
			)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
