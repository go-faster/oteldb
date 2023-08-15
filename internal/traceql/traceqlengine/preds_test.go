// Package traceqlengine implements TraceQL evaluation engine.
package traceqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/traceql"
)

func TestPredicateExtraction(t *testing.T) {
	tests := []struct {
		input string
		want  SelectSpansetsParams
	}{
		// Empty spanset selectors.
		{`{}`, SelectSpansetsParams{Op: traceql.SpansetOpAnd}},
		{`{} && {}`, SelectSpansetsParams{Op: traceql.SpansetOpUnion}},
		{`({} | coalesce()) && ({} | coalesce())`, SelectSpansetsParams{Op: traceql.SpansetOpUnion}},

		// Simple field expressions.
		{
			`{ .a = 10 }`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			// TODO(tdakkota): handle boolean attributes smarter: generate { .bool_attr = true }.
			`{ .bool_attr }`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{Attribute: traceql.Attribute{Name: "bool_attr"}},
				},
			},
		},
		{
			`{ -(.a + 10) > 10 }`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{Attribute: traceql.Attribute{Name: "a"}},
				},
			},
		},

		// Flip test.
		{
			`{ 10 = .a }`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ .a > 10 }`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpGt,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ 10 < .a }`, // -> { .a > 10 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpGt,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ .a < 10 }`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpLt,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ 10 > .a }`, // -> { .a < 10 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpLt,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},

		// NOT expression test.
		{
			`{ !(.a = 10) }`, // -> { .a != 10 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpNotEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a > 10) }`, // -> { .a <= 10 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpLte,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a < 10) }`, // -> { .a >= 10 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpGte,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a >= 10) }`, // -> { .a < 10 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpLt,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a <= 10) }`, // -> { .a > 10 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpGt,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a != 10) }`, // -> { .a = 10 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !!(.a = 10) }`, // -> { .a != 10 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !!(.a >= 10) }`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpGte,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !!(.a <= 10) }`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpLte,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		// https://en.wikipedia.org/wiki/De_Morgan%27s_laws
		{
			`{ !(.a = 10 || .b = 15) }`, // -> { .a != 10 && .b != 15 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpNotEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
					{
						Attribute: traceql.Attribute{Name: "b"},
						Op:        traceql.OpNotEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 15},
					},
				},
			},
		},
		{
			`{ !(.a =~ "foo" || .b !~ "bar") }`, // -> { .a !~ "foo" && .b =~ 15 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpNotRe,
						Static:    traceql.Static{Type: traceql.TypeString, Str: "foo"},
					},
					{
						Attribute: traceql.Attribute{Name: "b"},
						Op:        traceql.OpRe,
						Static:    traceql.Static{Type: traceql.TypeString, Str: "bar"},
					},
				},
			},
		},
		{
			`{ !(.a = 10 && .b = 15) }`, // -> { .a != 10 || .b != 15 }
			SelectSpansetsParams{
				Op: traceql.SpansetOpUnion,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpNotEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
					{
						Attribute: traceql.Attribute{Name: "b"},
						Op:        traceql.OpNotEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 15},
					},
				},
			},
		},

		// Operation expressions tests.
		{
			`{ .a = 10 } | select(.b, .c) | by(.d)`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
					{
						Attribute: traceql.Attribute{Name: "b"},
					},
					{
						Attribute: traceql.Attribute{Name: "c"},
					},
					{
						Attribute: traceql.Attribute{Name: "d"},
					},
				},
			},
		},

		// Scalar filter test.
		{
			`{ .a = 10 } | avg(.b) > 10`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
					{
						Attribute: traceql.Attribute{Name: "b"},
					},
				},
			},
		},
		{
			`{ .a = 10 } | min(duration)+min(traceDuration) > 10`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
					{
						Attribute: traceql.Attribute{Prop: traceql.SpanDuration},
					},
					{
						Attribute: traceql.Attribute{Prop: traceql.TraceDuration},
					},
				},
			},
		},

		// Binary spanset expression test.
		{
			`{ .a = 10 } && { .b = 10 }`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpUnion,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
					{
						Attribute: traceql.Attribute{Name: "b"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`({ .a = 10 } | coalesce()) && ({ .b = 10 } | coalesce())`,
			SelectSpansetsParams{
				Op: traceql.SpansetOpUnion,
				Matchers: []SpanMatcher{
					{
						Attribute: traceql.Attribute{Name: "a"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
					{
						Attribute: traceql.Attribute{Name: "b"},
						Op:        traceql.OpEq,
						Static:    traceql.Static{Type: traceql.TypeInt, Data: 10},
					},
				},
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			expr, err := traceql.Parse(tt.input)
			require.NoError(t, err)

			p := &predExtractor{}
			p.Walk(expr)
			require.Equal(t, tt.want, p.params)
		})
	}
}
