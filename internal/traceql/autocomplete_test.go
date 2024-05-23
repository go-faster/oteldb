package traceql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAutocomplete(t *testing.T) {
	tests := []struct {
		input string
		want  []BinaryFieldExpr
	}{
		{
			``,
			nil,
		},
		{
			`{}`,
			nil,
		},
		{
			`{ .a = }`,
			nil,
		},
		{
			`{ .a = `,
			nil,
		},
		// Simple cases.
		{
			`{ .a = 10 }`,
			[]BinaryFieldExpr{
				{
					Left:  &Attribute{Name: "a"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 10},
				},
			},
		},
		{
			`{ .a = 10 && .b =~ "foo.+" }`,
			[]BinaryFieldExpr{
				{
					Left:  &Attribute{Name: "a"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 10},
				},
				{
					Left:  &Attribute{Name: "b"},
					Op:    OpRe,
					Right: &Static{Type: TypeString, Str: "foo.+"},
				},
			},
		},
		// Missing brace.
		{
			`{ .a = 10 `,
			[]BinaryFieldExpr{
				{
					Left:  &Attribute{Name: "a"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 10},
				},
			},
		},
		// Missing sub-expression.
		{
			`{ .a = && .b = 20 && .c = 30 }`,
			[]BinaryFieldExpr{
				{
					Left:  &Attribute{Name: "b"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 20},
				},
				{
					Left:  &Attribute{Name: "c"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 30},
				},
			},
		},
		{
			`{ .a = 10 && .b = && .c = 30 }`,
			[]BinaryFieldExpr{
				{
					Left:  &Attribute{Name: "a"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 10},
				},
				{
					Left:  &Attribute{Name: "c"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 30},
				},
			},
		},
		{
			`{ .a = 10 && .b = 20 && .c = }`,
			[]BinaryFieldExpr{
				{
					Left:  &Attribute{Name: "a"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 10},
				},
				{
					Left:  &Attribute{Name: "b"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 20},
				},
			},
		},
		{
			`{ .a = && .b = && .c = 30 }`,
			[]BinaryFieldExpr{
				{
					Left:  &Attribute{Name: "c"},
					Op:    OpEq,
					Right: &Static{Type: TypeInt, Data: 30},
				},
			},
		},
		// Contains OR operation.
		{
			`{ .a = 10 && .b = 20 || .c = 30 }`,
			nil,
		},
		{
			`{ .a = 10 && .b =  || .c = 30 }`,
			nil,
		},
		// Complicated sub-expression.
		{
			`{ .status = 2*100 && .baz = 10 }`,
			nil,
		},
		{
			`{ .status / 100 = 2 && .baz = 10 }`,
			nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Input: %#q", tt.input)
				}
			}()

			got := ParseAutocomplete(tt.input)
			require.Equal(t, tt.want, got.Matchers)
		})
	}
}
