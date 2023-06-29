package logql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ptrTo[T any](v T) *T {
	return &v
}

func TestParse(t *testing.T) {
	tests := []struct {
		input   string
		want    Expr
		wantErr bool
	}{
		{`{}`, &LogExpr{}, false},
		{
			`{foo="bar"}`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", CmpEq, "bar"},
					},
				},
			},
			false,
		},
		{
			`{foo = "bar"}`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", CmpEq, "bar"},
					},
				},
			},
			false,
		},
		{
			`{foo!="bar"}`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", CmpNotEq, "bar"},
					},
				},
			},
			false,
		},
		{
			`{foo != "bar"}`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", CmpNotEq, "bar"},
					},
				},
			},
			false,
		},
		{
			`{foo =~ "bar"}`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", CmpRe, "bar"},
					},
				},
			},
			false,
		},
		{
			`{foo !~ "bar"}`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", CmpNotRe, "bar"},
					},
				},
			},
			false,
		},
		{
			`{foo !~ "bar", foo2 =~ "amongus"}`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", CmpNotRe, "bar"},
						{"foo2", CmpRe, "amongus"},
					},
				},
			},
			false,
		},
		{
			`( {foo = "bar"} )`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", CmpEq, "bar"},
					},
				},
			},
			false,
		},
		{
			"{} |= `foo`",
			&LogExpr{
				Pipeline: []PipelineStage{
					&LineFilter{Op: CmpEq, Value: "foo"},
				},
			},
			false,
		},
		{
			`{instance=~"kafka-1",name="kafka"}
				|= "bad"
				|~ "error"
				!= "good"
				!~ "exception"`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"instance", CmpRe, "kafka-1"},
						{"name", CmpEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					&LineFilter{Op: CmpEq, Value: "bad"},
					&LineFilter{Op: CmpRe, Value: "error"},
					&LineFilter{Op: CmpNotEq, Value: "good"},
					&LineFilter{Op: CmpNotRe, Value: "exception"},
				},
			},
			false,
		},
		{
			`( {instance=~"kafka-1",name="kafka"} |= "bad" )`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"instance", CmpRe, "kafka-1"},
						{"name", CmpEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					&LineFilter{Op: CmpEq, Value: "bad"},
				},
			},
			false,
		},
		{
			`{name="kafka"}
				|= "bad"
				| logfmt
				| json
				| regexp ".*"
				| pattern "<ip>"
				| unpack
				| line_format "{{ . }}"
				| decolorize`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"name", CmpEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					&LineFilter{Op: CmpEq, Value: "bad"},
					&LogfmtExpressionParser{},
					&JSONExpressionParser{},
					&RegexpLabelParser{Regexp: ".*"},
					&PatternLabelParser{Pattern: "<ip>"},
					&UnpackLabelParser{},
					&LineFormat{Template: "{{ . }}"},
					&DecolorizeExpr{},
				},
			},
			false,
		},
		{
			`{name="kafka"}
				|= "bad"
				| json
				| json foo, bar
				| json foo="10", bar="sus"
				| logfmt foo="10", bar="sus"
			`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"name", CmpEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					&LineFilter{Op: CmpEq, Value: "bad"},
					&JSONExpressionParser{},
					&JSONExpressionParser{
						Labels: []Label{
							"foo",
							"bar",
						},
					},
					&JSONExpressionParser{
						Exprs: []LabelExtractionExpr{
							{Label: "foo", EqTo: "10"},
							{Label: "bar", EqTo: "sus"},
						},
					},
					&LogfmtExpressionParser{
						Exprs: []LabelExtractionExpr{
							{Label: "foo", EqTo: "10"},
							{Label: "bar", EqTo: "sus"},
						},
					},
				},
			},
			false,
		},
		{
			`{name="kafka"}
				| drop foo
				| drop foo, foo2
				| keep foo=~"bar"
				| keep foo=~"bar", foo2=~"baz"
				| drop foo,foo2=~"bar",foo3
				| keep foo!~"bar",foo2,foo3
			`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"name", CmpEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					&DropLabelsExpr{
						Labels: []Label{"foo"},
					},
					&DropLabelsExpr{
						Labels: []Label{"foo", "foo2"},
					},
					&KeepLabelsExpr{
						Matchers: []LabelMatcher{
							{Label: "foo", Op: CmpRe, Value: "bar"},
						},
					},
					&KeepLabelsExpr{
						Matchers: []LabelMatcher{
							{Label: "foo", Op: CmpRe, Value: "bar"},
							{Label: "foo2", Op: CmpRe, Value: "baz"},
						},
					},
					&DropLabelsExpr{
						Labels: []Label{"foo", "foo3"},
						Matchers: []LabelMatcher{
							{Label: "foo2", Op: CmpRe, Value: "bar"},
						},
					},
					&KeepLabelsExpr{
						Labels: []Label{"foo2", "foo3"},
						Matchers: []LabelMatcher{
							{Label: "foo", Op: CmpNotRe, Value: "bar"},
						},
					},
				},
			},
			false,
		},
		{
			`{name="kafka"}
				| label_format foo=foo
				| label_format bar="bar"
				| label_format foo=foo,bar="bar"
			`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"name", CmpEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					&LabelFormatExpr{
						Labels: []LabelFormatLabel{
							{"foo", "foo"},
						},
					},
					&LabelFormatExpr{
						Values: []LabelFormatValue{
							{"bar", "bar"},
						},
					},
					&LabelFormatExpr{
						Labels: []LabelFormatLabel{
							{"foo", "foo"},
						},
						Values: []LabelFormatValue{
							{"bar", "bar"},
						},
					},
				},
			},
			false,
		},
		{
			`{name="kafka"}
				| distinct foo
				| distinct foo,bar
				| distinct foo,bar,baz
			`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"name", CmpEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					&DistinctFilter{
						Labels: []Label{"foo"},
					},
					&DistinctFilter{
						Labels: []Label{"foo", "bar"},
					},

					&DistinctFilter{
						Labels: []Label{"foo", "bar", "baz"},
					},
				},
			},
			false,
		},
		{
			`{instance=~"kafka-1",name="kafka"}
				| status == 200
				| (service = "sus1")
				| service = "sus2", request != "GET"
				| service = "sus3" request != "POST"
				| ( (service = "sus4") and request != "PUT" )`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"instance", CmpRe, "kafka-1"},
						{"name", CmpEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					&LabelFilter{
						Pred: &NumberFilter{
							Label: "status",
							Op:    CmpEq,
							Value: 200,
						},
					},
					&LabelFilter{
						Pred: &LabelPredicateParen{
							X: &LabelMatcher{
								Label: "service",
								Op:    CmpEq,
								Value: "sus1",
							},
						},
					},
					&LabelFilter{
						Pred: &LabelPredicateBinOp{
							Left: &LabelMatcher{
								Label: "service",
								Op:    CmpEq,
								Value: "sus2",
							},
							Op: LogOpAnd,
							Right: &LabelMatcher{
								Label: "request",
								Op:    CmpNotEq,
								Value: "GET",
							},
						},
					},
					&LabelFilter{
						Pred: &LabelPredicateBinOp{
							Left: &LabelMatcher{
								Label: "service",
								Op:    CmpEq,
								Value: "sus3",
							},
							Op: LogOpAnd,
							Right: &LabelMatcher{
								Label: "request",
								Op:    CmpNotEq,
								Value: "POST",
							},
						},
					},
					&LabelFilter{
						Pred: &LabelPredicateParen{
							X: &LabelPredicateBinOp{
								Left: &LabelPredicateParen{
									X: &LabelMatcher{
										Label: "service",
										Op:    CmpEq,
										Value: "sus4",
									},
								},
								Op: LogOpAnd,
								Right: &LabelMatcher{
									Label: "request",
									Op:    CmpNotEq,
									Value: "PUT",
								},
							},
						},
					},
				},
			},
			false,
		},
		{
			`{instance=~"kafka-1",name="kafka"}
				| duration >= 20ms or size == 20kb and method!~"2.."
				| ip == ip("127.0.0.1")`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"instance", CmpRe, "kafka-1"},
						{"name", CmpEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					&LabelFilter{
						Pred: &LabelPredicateBinOp{
							Left: &DurationFilter{
								Label: "duration",
								Op:    CmpGte,
								Value: 20 * time.Millisecond,
							},
							Op: LogOpOr,
							Right: &LabelPredicateBinOp{
								Left: &BytesFilter{
									Label: "size",
									Op:    CmpEq,
									Value: 20 * 1000, // 20kb
								},
								Op: LogOpAnd,
								Right: &LabelMatcher{
									Label: "method",
									Op:    CmpNotRe,
									Value: "2..",
								},
							},
						},
					},
					&LabelFilter{
						Pred: &IPFilter{
							Label: "ip",
							Op:    CmpEq,
							Value: "127.0.0.1",
						},
					},
				},
			},
			false,
		},

		// Metric queries.
		// Range aggregation.
		{
			`count_over_time({job="mysql"}[5m] offset 15m)`,
			&RangeAggregationExpr{
				Op: RangeOpCount,
				Range: LogRangeExpr{
					Sel: Selector{
						Matchers: []LabelMatcher{
							{"job", CmpEq, "mysql"},
						},
					},
					Range: 5 * time.Minute,
					Offset: &OffsetExpr{
						Duration: 15 * time.Minute,
					},
				},
			},
			false,
		},
		{
			`avg_over_time({ job = "mysql" }[5h]) without (foo)`,
			&RangeAggregationExpr{
				Op: RangeOpAvg,
				Range: LogRangeExpr{
					Sel: Selector{
						Matchers: []LabelMatcher{
							{"job", CmpEq, "mysql"},
						},
					},
					Range: 5 * time.Hour,
				},
				Grouping: &Grouping{
					Op:     GroupingOpWithout,
					Labels: []Label{"foo"},
				},
			},
			false,
		},
		{
			`avg_over_time(10, { job = "mysql" }[5h] |= "error" | logfmt) by (bar,foo)`,
			&RangeAggregationExpr{
				Op: RangeOpAvg,
				Range: LogRangeExpr{
					Sel: Selector{
						Matchers: []LabelMatcher{
							{"job", CmpEq, "mysql"},
						},
					},
					Pipeline: []PipelineStage{
						&LineFilter{Op: CmpEq, Value: "error"},
						&LogfmtExpressionParser{},
					},
					Range: 5 * time.Hour,
				},
				Parameter: ptrTo(10.0),
				Grouping: &Grouping{
					Op:     GroupingOpBy,
					Labels: []Label{"bar", "foo"},
				},
			},
			false,
		},
		{
			`avg_over_time({ job = "mysql" }[5h] |= "error" | unwrap duration)`,
			&RangeAggregationExpr{
				Op: RangeOpAvg,
				Range: LogRangeExpr{
					Sel: Selector{
						Matchers: []LabelMatcher{
							{"job", CmpEq, "mysql"},
						},
					},
					Pipeline: []PipelineStage{
						&LineFilter{Op: CmpEq, Value: "error"},
					},
					Unwrap: &UnwrapExpr{
						Label: "duration",
					},
					Range: 5 * time.Hour,
				},
			},
			false,
		},
		{
			`avg_over_time({ job = "mysql" }[5h] |= "error" | unwrap duration(bytes) | foo="bar")`,
			&RangeAggregationExpr{
				Op: RangeOpAvg,
				Range: LogRangeExpr{
					Sel: Selector{
						Matchers: []LabelMatcher{
							{"job", CmpEq, "mysql"},
						},
					},
					Pipeline: []PipelineStage{
						&LineFilter{Op: CmpEq, Value: "error"},
					},
					Unwrap: &UnwrapExpr{
						Op:    "duration",
						Label: "bytes",
						Filters: []LabelMatcher{
							{Label: "foo", Op: CmpEq, Value: "bar"},
						},
					},
					Range: 5 * time.Hour,
				},
			},
			false,
		},
		// Vector aggregation.
		{
			`sum by (host) (rate({job="mysql"} |= "error" != "timeout" | json | duration > 10s [1m]))`,
			&VectorAggregationExpr{
				Op: VectorOpSum,
				Expr: &RangeAggregationExpr{
					Op: RangeOpRate,
					Range: LogRangeExpr{
						Sel: Selector{
							Matchers: []LabelMatcher{
								{"job", CmpEq, "mysql"},
							},
						},
						Pipeline: []PipelineStage{
							&LineFilter{Op: CmpEq, Value: "error"},
							&LineFilter{Op: CmpNotEq, Value: "timeout"},
							&JSONExpressionParser{},
							&LabelFilter{
								Pred: &DurationFilter{
									Label: "duration",
									Op:    CmpGt,
									Value: 10 * time.Second,
								},
							},
						},
						Range: 1 * time.Minute,
					},
				},
				Grouping: &Grouping{
					Op:     GroupingOpBy,
					Labels: []Label{"host"},
				},
			},
			false,
		},
		// label_replace
		{
			`label_replace(rate({}), "dst", "replacement", "src", ".*")`,
			&LabelReplaceExpr{
				Expr: &RangeAggregationExpr{
					Op: RangeOpRate,
				},
				DstLabel:    "dst",
				Replacement: "replacement",
				SrcLabel:    "src",
				Regex:       ".*",
			},
			false,
		},
		// Literal expression.
		{
			`10.0`,
			&LiteralExpr{
				Value: 10.0,
			},
			false,
		},
		{
			`+10.0`,
			&LiteralExpr{
				Value: 10.0,
			},
			false,
		},
		{
			`-10.0`,
			&LiteralExpr{
				Value: -10.0,
			},
			false,
		},
		// Vector expression.
		{
			`vector (10.0)`,
			&VectorExpr{
				Value: 10.0,
			},
			false,
		},

		{"{", nil, true},
		{"{foo}", nil, true},
		{"{foo =}", nil, true},
		{`{foo == "bar"}`, nil, true},
		{`{foo = bar}`, nil, true},
		{`{foo = "bar"} | addr >= ip("127.0.0.1")`, nil, true},
		{`{foo = "bar"} | foo == "bar"`, nil, true},
		{`{foo = "bar"} | status = 10`, nil, true},
	}
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
