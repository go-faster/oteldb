package traceql

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func ptrTo[T any](v T) *T {
	return &v
}

type TestCase struct {
	input   string
	want    Expr
	wantErr bool
}

func testBinFieldExpr(left FieldExpr, op BinaryOp, right FieldExpr) Expr {
	return &SpansetPipeline{
		Pipeline: []PipelineStage{
			&SpansetFilter{
				Expr: &BinaryFieldExpr{
					Left:  left,
					Op:    op,
					Right: right,
				},
			},
		},
	}
}

var noConst = 0

var tests = []TestCase{
	{
		`{}`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&SpansetFilter{
					Expr: &Static{Type: StaticBool, Data: 1},
				},
			},
		},
		false,
	},
	{
		`{ rootServiceName = "bar" }`,
		testBinFieldExpr(
			&Attribute{Prop: RootServiceName},
			OpEq,
			&Static{Type: StaticString, Str: "bar"},
		),
		false,
	},
	{
		`{ rootName != "bar" }`,
		testBinFieldExpr(
			&Attribute{Prop: RootSpanName},
			OpNotEq,
			&Static{Type: StaticString, Str: "bar"},
		),
		false,
	},
	{
		`{ name =~ "bar" }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanName},
			OpRe,
			&Static{Type: StaticString, Str: "bar"},
		),
		false,
	},
	{
		`{ childCount = 10 }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanChildCount},
			OpEq,
			&Static{Type: StaticInteger, Data: 10},
		),
		false,
	},
	{
		`{ .foo >= -10 }`,
		testBinFieldExpr(
			&Attribute{Name: "foo"},
			OpGte,
			&Static{Type: StaticInteger, Data: uint64(-10 + noConst)},
		),
		false,
	},
	{
		`{ .foo <= .5 }`,
		testBinFieldExpr(
			&Attribute{Name: "foo"},
			OpLte,
			&Static{Type: StaticNumber, Data: math.Float64bits(.5)},
		),
		false,
	},
	{
		`{ .foo && true }`,
		testBinFieldExpr(
			&Attribute{Name: "foo"},
			OpAnd,
			&Static{Type: StaticBool, Data: 1},
		),
		false,
	},
	{
		`{ .foo && false }`,
		testBinFieldExpr(
			&Attribute{Name: "foo"},
			OpAnd,
			&Static{Type: StaticBool, Data: 0},
		),
		false,
	},
	{
		`{ .foo = nil }`,
		testBinFieldExpr(
			&Attribute{Name: "foo"},
			OpEq,
			&Static{Type: StaticNil},
		),
		false,
	},
	{
		`{ duration > 10s }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanDuration},
			OpGt,
			&Static{Type: StaticDuration, Data: uint64(10 * time.Second)},
		),
		false,
	},
	{
		`{ traceDuration < 1d }`,
		testBinFieldExpr(
			&Attribute{Prop: TraceDuration},
			OpLt,
			&Static{Type: StaticDuration, Data: uint64(24 * time.Hour)},
		),
		false,
	},
	{
		`{ status = ok }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanStatus},
			OpEq,
			&Static{Type: StaticSpanStatus, Data: uint64(ptrace.StatusCodeOk)},
		),
		false,
	},
	{
		`{ status = unset }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanStatus},
			OpEq,
			&Static{Type: StaticSpanStatus, Data: uint64(ptrace.StatusCodeUnset)},
		),
		false,
	},
	{
		`{ status = error }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanStatus},
			OpEq,
			&Static{Type: StaticSpanStatus, Data: uint64(ptrace.StatusCodeError)},
		),
		false,
	},
	{
		`{ kind = unspecified }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanKind},
			OpEq,
			&Static{Type: StaticSpanKind, Data: uint64(ptrace.SpanKindUnspecified)},
		),
		false,
	},
	{
		`{ kind = internal }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanKind},
			OpEq,
			&Static{Type: StaticSpanKind, Data: uint64(ptrace.SpanKindInternal)},
		),
		false,
	},
	{
		`{ kind = server }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanKind},
			OpEq,
			&Static{Type: StaticSpanKind, Data: uint64(ptrace.SpanKindServer)},
		),
		false,
	},
	{
		`{ kind = client }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanKind},
			OpEq,
			&Static{Type: StaticSpanKind, Data: uint64(ptrace.SpanKindClient)},
		),
		false,
	},
	{
		`{ kind = producer }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanKind},
			OpEq,
			&Static{Type: StaticSpanKind, Data: uint64(ptrace.SpanKindProducer)},
		),
		false,
	},
	{
		`{ kind = consumer }`,
		testBinFieldExpr(
			&Attribute{Prop: SpanKind},
			OpEq,
			&Static{Type: StaticSpanKind, Data: uint64(ptrace.SpanKindConsumer)},
		),
		false,
	},
	{
		`{ (kind = client) }`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&SpansetFilter{
					Expr: &ParenFieldExpr{
						&BinaryFieldExpr{
							&Attribute{Prop: SpanKind},
							OpEq,
							&Static{Type: StaticSpanKind, Data: uint64(ptrace.SpanKindClient)},
						},
					},
				},
			},
		},
		false,
	},
	{
		`{ -(childCount) < 0 }`,
		testBinFieldExpr(
			&UnaryFieldExpr{
				Op:   OpNeg,
				Expr: &ParenFieldExpr{Expr: &Attribute{Prop: SpanChildCount}},
			},
			OpLt,
			&Static{Type: StaticInteger, Data: 0},
		),
		false,
	},
	{
		`{ .github.com/ogen-go/ogen.attr =~ "foo" }`,
		testBinFieldExpr(
			&Attribute{Name: "github.com/ogen-go/ogen.attr"},
			OpRe,
			&Static{Type: StaticString, Str: "foo"},
		),
		false,
	},
	{
		`{ resource.github.com/ogen-go/ogen.attr !~ "foo" }`,
		testBinFieldExpr(
			&Attribute{Name: "github.com/ogen-go/ogen.attr", Scope: ScopeResource},
			OpNotRe,
			&Static{Type: StaticString, Str: "foo"},
		),
		false,
	},
	{
		`{ .a || span.a || resource.a } && { parent.a && parent.span.a && parent.resource.a }`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&BinarySpansetExpr{
					Left: &SpansetFilter{
						Expr: &BinaryFieldExpr{
							Left: &Attribute{Name: "a"},
							Op:   OpOr,
							Right: &BinaryFieldExpr{
								Left:  &Attribute{Name: "a", Scope: ScopeSpan},
								Op:    OpOr,
								Right: &Attribute{Name: "a", Scope: ScopeResource},
							},
						},
					},
					Op: SpansetOpAnd,
					Right: &SpansetFilter{
						Expr: &BinaryFieldExpr{
							Left: &Attribute{Name: "a", Parent: true},
							Op:   OpAnd,
							Right: &BinaryFieldExpr{
								Left:  &Attribute{Name: "a", Scope: ScopeSpan, Parent: true},
								Op:    OpAnd,
								Right: &Attribute{Name: "a", Scope: ScopeResource, Parent: true},
							},
						},
					},
				},
			},
		},
		false,
	},
	{
		`{ .foo.bar + span.foo.bar - resource.foo.bar } && { parent.foo.bar / parent.span.foo.bar * parent.resource.foo.bar }`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&BinarySpansetExpr{
					Left: &SpansetFilter{
						Expr: &BinaryFieldExpr{
							Left: &Attribute{Name: "foo.bar"},
							Op:   OpAdd,
							Right: &BinaryFieldExpr{
								Left:  &Attribute{Name: "foo.bar", Scope: ScopeSpan},
								Op:    OpSub,
								Right: &Attribute{Name: "foo.bar", Scope: ScopeResource},
							},
						},
					},
					Op: SpansetOpAnd,
					Right: &SpansetFilter{
						Expr: &BinaryFieldExpr{
							Left: &Attribute{Name: "foo.bar", Parent: true},
							Op:   OpDiv,
							Right: &BinaryFieldExpr{
								Left:  &Attribute{Name: "foo.bar", Scope: ScopeSpan, Parent: true},
								Op:    OpMul,
								Right: &Attribute{Name: "foo.bar", Scope: ScopeResource, Parent: true},
							},
						},
					},
				},
			},
		},
		false,
	},
	{
		`({ .a } || { .b }) > { .c }`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&BinarySpansetExpr{
					Left: &BinarySpansetExpr{
						Left: &SpansetFilter{
							Expr: &Attribute{Name: "a"},
						},
						Op: SpansetOpUnion,
						Right: &SpansetFilter{
							Expr: &Attribute{Name: "b"},
						},
					},
					Op: SpansetOpChild,
					Right: &SpansetFilter{
						Expr: &Attribute{Name: "c"},
					},
				},
			},
		},
		false,
	},
	{
		`{ .a } && { .b } ~ { .c }`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&BinarySpansetExpr{
					Left: &SpansetFilter{
						Expr: &Attribute{Name: "a"},
					},
					Op: SpansetOpAnd,
					Right: &BinarySpansetExpr{
						Left: &SpansetFilter{
							Expr: &Attribute{Name: "b"},
						},
						Op: SpansetOpSibling,
						Right: &SpansetFilter{
							Expr: &Attribute{Name: "c"},
						},
					},
				},
			},
		},
		false,
	},
	{
		`{ .a } && { .b } >> { .c } && { .d }`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&BinarySpansetExpr{
					Left: &SpansetFilter{
						Expr: &Attribute{Name: "a"},
					},
					Op: SpansetOpAnd,
					Right: &BinarySpansetExpr{
						Left: &BinarySpansetExpr{
							Left: &SpansetFilter{
								Expr: &Attribute{Name: "b"},
							},
							Op: SpansetOpDescendant,
							Right: &SpansetFilter{
								Expr: &Attribute{Name: "c"},
							},
						},
						Op: SpansetOpAnd,
						Right: &SpansetFilter{
							Expr: &Attribute{Name: "d"},
						},
					},
				},
			},
		},
		false,
	},
	{
		`-2 = -2`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&ScalarFilter{
					Left:  &Static{Type: StaticInteger, Data: uint64(-2 + noConst)},
					Op:    OpEq,
					Right: &Static{Type: StaticInteger, Data: uint64(-2 + noConst)},
				},
			},
		},
		false,
	},
	{
		`(1+2)^3 = 27`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&ScalarFilter{
					Left: &BinaryScalarExpr{
						Left: &BinaryScalarExpr{
							Left:  &Static{Type: StaticInteger, Data: uint64(1)},
							Op:    OpAdd,
							Right: &Static{Type: StaticInteger, Data: uint64(2)},
						},
						Op:    OpPow,
						Right: &Static{Type: StaticInteger, Data: uint64(3)},
					},
					Op:    OpEq,
					Right: &Static{Type: StaticInteger, Data: uint64(27)},
				},
			},
		},
		false,
	},
	{
		`1+2*3^4 = 163`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&ScalarFilter{
					Left: &BinaryScalarExpr{
						Left: &Static{Type: StaticInteger, Data: uint64(1)},
						Op:   OpAdd,
						Right: &BinaryScalarExpr{
							Left: &Static{Type: StaticInteger, Data: uint64(2)},
							Op:   OpMul,
							Right: &BinaryScalarExpr{
								Left:  &Static{Type: StaticInteger, Data: uint64(3)},
								Op:    OpPow,
								Right: &Static{Type: StaticInteger, Data: uint64(4)},
							},
						},
					},
					Op:    OpEq,
					Right: &Static{Type: StaticInteger, Data: uint64(163)},
				},
			},
		},
		false,
	},
	{
		`{ 1+2*3^4 }`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&SpansetFilter{
					Expr: &BinaryFieldExpr{
						Left: &Static{Type: StaticInteger, Data: uint64(1)},
						Op:   OpAdd,
						Right: &BinaryFieldExpr{
							Left: &Static{Type: StaticInteger, Data: uint64(2)},
							Op:   OpMul,
							Right: &BinaryFieldExpr{
								Left:  &Static{Type: StaticInteger, Data: uint64(3)},
								Op:    OpPow,
								Right: &Static{Type: StaticInteger, Data: uint64(4)},
							},
						},
					},
				},
			},
		},
		false,
	},
	{
		`2+3*4+5 = 19`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&ScalarFilter{
					Left: &BinaryScalarExpr{
						Left: &Static{Type: StaticInteger, Data: uint64(2)},
						Op:   OpAdd,
						Right: &BinaryScalarExpr{
							Left: &BinaryScalarExpr{
								Left:  &Static{Type: StaticInteger, Data: uint64(3)},
								Op:    OpMul,
								Right: &Static{Type: StaticInteger, Data: uint64(4)},
							},
							Op:    OpAdd,
							Right: &Static{Type: StaticInteger, Data: uint64(5)},
						},
					},
					Op:    OpEq,
					Right: &Static{Type: StaticInteger, Data: uint64(19)},
				},
			},
		},
		false,
	},
	{
		`max(.foo) > count() + sum(.bar)`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&ScalarFilter{
					Left: &AggregateScalarExpr{Op: AggregateOpMax, Field: &Attribute{Name: "foo"}},
					Op:   OpGt,
					Right: &BinaryScalarExpr{
						Left:  &AggregateScalarExpr{Op: AggregateOpCount},
						Op:    OpAdd,
						Right: &AggregateScalarExpr{Op: AggregateOpSum, Field: &Attribute{Name: "bar"}},
					},
				},
			},
		},
		false,
	},
	{
		`min(.foo) + count() > sum(.bar)`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&ScalarFilter{
					Left: &BinaryScalarExpr{
						Left:  &AggregateScalarExpr{Op: AggregateOpMin, Field: &Attribute{Name: "foo"}},
						Op:    OpAdd,
						Right: &AggregateScalarExpr{Op: AggregateOpCount},
					},
					Op:    OpGt,
					Right: &AggregateScalarExpr{Op: AggregateOpSum, Field: &Attribute{Name: "bar"}},
				},
			},
		},
		false,
	},
	{
		`{ .a } | by(.b) | coalesce() | select(.c, .d) | avg(duration) = 1s`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&SpansetFilter{
					Expr: &Attribute{Name: "a"},
				},
				&GroupOperation{
					By: &Attribute{Name: "b"},
				},
				&CoalesceOperation{},
				&SelectOperation{
					Args: []FieldExpr{
						&Attribute{Name: "c"},
						&Attribute{Name: "d"},
					},
				},
				&ScalarFilter{
					Left: &AggregateScalarExpr{
						Op:    AggregateOpAvg,
						Field: &Attribute{Prop: SpanDuration},
					},
					Op:    OpEq,
					Right: &Static{Type: StaticDuration, Data: uint64(time.Second)},
				},
			},
		},
		false,
	},
	{
		`( { .a } ) | by(.b)`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&SpansetFilter{
					Expr: &Attribute{Name: "a"},
				},
				&GroupOperation{
					By: &Attribute{Name: "b"},
				},
			},
		},
		false,
	},
	{
		`{ .a } && { .b } | by(.b)`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&BinarySpansetExpr{
					Left: &SpansetFilter{
						Expr: &Attribute{Name: "a"},
					},
					Op: SpansetOpAnd,
					Right: &SpansetFilter{
						Expr: &Attribute{Name: "b"},
					},
				},
				&GroupOperation{
					By: &Attribute{Name: "b"},
				},
			},
		},
		false,
	},
	{
		`( { .a } && { .b } ) | by(.b)`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&BinarySpansetExpr{
					Left: &SpansetFilter{
						Expr: &Attribute{Name: "a"},
					},
					Op: SpansetOpAnd,
					Right: &SpansetFilter{
						Expr: &Attribute{Name: "b"},
					},
				},
				&GroupOperation{
					By: &Attribute{Name: "b"},
				},
			},
		},
		false,
	},
	{
		`( { .a } | by(.b) ) ~ ( { .b } | by(.b) )`,
		&BinaryExpr{
			Left: &SpansetPipeline{
				Pipeline: []PipelineStage{
					&SpansetFilter{
						Expr: &Attribute{Name: "a"},
					},
					&GroupOperation{
						By: &Attribute{Name: "b"},
					},
				},
			},
			Op: SpansetOpSibling,
			Right: &SpansetPipeline{
				Pipeline: []PipelineStage{
					&SpansetFilter{
						Expr: &Attribute{Name: "b"},
					},
					&GroupOperation{
						By: &Attribute{Name: "b"},
					},
				},
			},
		},
		false,
	},
	{
		`( { .a } | coalesce() ) && ( { .b } | coalesce() ) >> ( { .c } | coalesce() ) && ( { .d } | coalesce() )`,
		&BinaryExpr{
			Left: &SpansetPipeline{
				Pipeline: []PipelineStage{
					&SpansetFilter{
						Expr: &Attribute{Name: "a"},
					},
					&CoalesceOperation{},
				},
			},
			Op: SpansetOpAnd,
			Right: &BinaryExpr{
				Left: &BinaryExpr{
					Left: &SpansetPipeline{
						Pipeline: []PipelineStage{
							&SpansetFilter{
								Expr: &Attribute{Name: "b"},
							},
							&CoalesceOperation{},
						},
					},
					Op: SpansetOpDescendant, // Higher precedence then and.
					Right: &SpansetPipeline{
						Pipeline: []PipelineStage{
							&SpansetFilter{
								Expr: &Attribute{Name: "c"},
							},
							&CoalesceOperation{},
						},
					},
				},
				Op: SpansetOpAnd,
				Right: &SpansetPipeline{
					Pipeline: []PipelineStage{
						&SpansetFilter{
							Expr: &Attribute{Name: "d"},
						},
						&CoalesceOperation{},
					},
				},
			},
		},
		false,
	},

	// Invalid syntax.
	{`{`, nil, true},
	{`{ .a`, nil, true},
	{`{ 1+ }`, nil, true},
	{`{ -- }`, nil, true},
	{`{ (1+) }`, nil, true},
	{`{ (1+1 }`, nil, true},
	{`{} | `, nil, true},
	{`{} | by`, nil, true},
	{`{} | coalesce`, nil, true},
	{`{} | select`, nil, true},
	{`{} | by(.foo`, nil, true},
	{`{} | coalesce(`, nil, true},
	{`{} | select(.foo`, nil, true},
	{`(`, nil, true},
	{`()`, nil, true},
	{`(( { .a } )`, nil, true},
	// Missing expression.
	{`{ .a = }`, nil, true},
	{`20 >`, nil, true},
	{`1 > 1+`, nil, true},
	{`{ .a } ~ `, nil, true},
	{`{ .a } && { .b } ~ `, nil, true},
	{`({ .a }) ~ `, nil, true},
	{`({ .a }) ~ ()`, nil, true},
	{`({ .a } | by(.b)) ~ `, nil, true},
	{`({ .a } | by(.b)) && ({ .b } | by(.b)) ~`, nil, true},
	// Parameter is required,
	{`{} | max()`, nil, true},
	{`{} | min()`, nil, true},
	{`{} | avg()`, nil, true},
	{`{} | sum()`, nil, true},
	{`{} | by()`, nil, true},
	{`{} | select()`, nil, true},
	{`{} | select(.foo,)`, nil, true},
	// Parameter is not allowed.
	{`{} | count(.foo) = 10`, nil, true},
	// Stage `coalesce` cannot be first stage.
	{`coalesce()`, nil, true},
	// Scalar filter must be a part of pipeline.
	{`max() > 3 && { }`, nil, true},
}

func TestParse(t *testing.T) {
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Input:\n%s", tt.input)
				}
			}()

			got, err := Parse(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func FuzzParse(f *testing.F) {
	for _, tt := range tests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if r := recover(); r != nil || t.Failed() {
				t.Logf("Input:\n%s", input)
			}

			_, _ = Parse(input)
		}()
	})
}
