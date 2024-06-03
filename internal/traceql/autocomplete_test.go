package traceql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var autocompleteTests = []struct {
	input string
	want  Autocomplete
}{
	{``, Autocomplete{}},
	{`{}`, Autocomplete{}},
	{`{ .a = }`, Autocomplete{}},
	{`{ .a = `, Autocomplete{}},
	// Contains OR operation.
	{`{ .a = 10 && .b = 20 || .c = 30 }`, Autocomplete{}},
	{`{ .a = 10 && .b =  || .c = 30 }`, Autocomplete{}},
	// Complicated sub-expression.
	{`{ .status = 2*100 && .baz = 10 }`, Autocomplete{}},
	{`{ .status / 100 = 2 && .baz = 10 }`, Autocomplete{}},
	// Invalid string.
	{`{ name =~ "\" }`, Autocomplete{}},
	// Invalid regex.
	{`{ name =~ 10 }`, Autocomplete{}},
	{`{ name =~ "\\" }`, Autocomplete{}},

	// Simple cases.
	{
		`{ .a = 10 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
			},
		},
	},
	{
		`{ .a > 10 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpGt,
					Static:    Static{Type: TypeInt, Data: 10},
				},
			},
		},
	},
	{
		`{ 10 > .a }`, // -> { .a < 10 }
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpLt,
					Static:    Static{Type: TypeInt, Data: 10},
				},
			},
		},
	},
	{
		`{ .a = 10 && .b =~ "foo.+" }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
				{
					Attribute: Attribute{Name: "b"},
					Op:        OpRe,
					Static:    Static{Type: TypeString, Str: "foo.+"},
				},
			},
		},
	},
	// Missing brace.
	{
		`{ .a = 10 `,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
			},
		},
	},
	// Missing sub-expression.
	{
		`{ .a = && .b = 20 && .c = 30 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "b"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 20},
				},
				{
					Attribute: Attribute{Name: "c"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 30},
				},
			},
		},
	},
	{
		`{ .a = 10 && .b = && .c = 30 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
				{
					Attribute: Attribute{Name: "c"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 30},
				},
			},
		},
	},
	{
		`{ .a = 10 && .b = 20 && .c = }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "a"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 10},
				},
				{
					Attribute: Attribute{Name: "b"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 20},
				},
			},
		},
	},
	{
		`{ .a = && .b = && .c = 30 }`,
		Autocomplete{
			[]SpanMatcher{
				{
					Attribute: Attribute{Name: "c"},
					Op:        OpEq,
					Static:    Static{Type: TypeInt, Data: 30},
				},
			},
		},
	},
}

func TestParseAutocomplete(t *testing.T) {
	for i, tt := range autocompleteTests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Input: %#q", tt.input)
				}
			}()

			got := ParseAutocomplete(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}
