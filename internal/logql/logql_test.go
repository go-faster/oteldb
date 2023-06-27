package logql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func ptrTo[T any](v T) *T {
	return &v
}

func TestParse(t *testing.T) {
	tests := []struct {
		input   string
		want    *LogExpr
		wantErr bool
	}{
		{`{}`, &LogExpr{}, false},
		{
			`{foo="bar"}`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", LabelEq, "bar"},
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
						{"foo", LabelEq, "bar"},
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
						{"foo", LabelNotEq, "bar"},
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
						{"foo", LabelNotEq, "bar"},
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
						{"foo", LabelRe, "bar"},
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
						{"foo", LabelNotRe, "bar"},
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
						{"foo", LabelNotRe, "bar"},
						{"foo2", LabelRe, "amongus"},
					},
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
						{"instance", LabelRe, "kafka-1"},
						{"name", LabelEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					{LineFilter: &LineFilter{Op: LineEq, Value: "bad"}},
					{LineFilter: &LineFilter{Op: LineRe, Value: "error"}},
					{LineFilter: &LineFilter{Op: LineNotEq, Value: "good"}},
					{LineFilter: &LineFilter{Op: LineNotRe, Value: "exception"}},
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
						{"name", LabelEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					{LineFilter: &LineFilter{Op: LineEq, Value: "bad"}},
					{Logfmt: &LogfmtExpressionParser{}},
					{JSON: &JSONExpressionParser{}},
					{Regexp: &RegexpLabelParser{Regexp: ".*"}},
					{Pattern: &PatternLabelParser{Pattern: "<ip>"}},
					{Unpack: true},
					{LineFormat: &LineFormat{Template: "{{ . }}"}},
					{Decolorize: true},
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
						{"name", LabelEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					{LineFilter: &LineFilter{Op: LineEq, Value: "bad"}},
					{JSON: &JSONExpressionParser{}},
					{JSON: &JSONExpressionParser{
						Exprs: []LabelExtractionExpr{
							{Label: "foo"},
							{Label: "bar"},
						},
					}},
					{JSON: &JSONExpressionParser{
						Exprs: []LabelExtractionExpr{
							{Label: "foo", EqTo: ptrTo("10")},
							{Label: "bar", EqTo: ptrTo("sus")},
						},
					}},
					{Logfmt: &LogfmtExpressionParser{
						Exprs: []LabelExtractionExpr{
							{Label: "foo", EqTo: ptrTo("10")},
							{Label: "bar", EqTo: ptrTo("sus")},
						},
					}},
				},
			},
			false,
		},
		{
			`{instance=~"kafka-1",name="kafka"}
				| (service = "sus")
				| service = "sus", request != "GET"
				| ( (service = "sus") and request != "GET" )
				| duration >= 20ms or size == 20kb and method!~"2.."`,
			&LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"instance", LabelRe, "kafka-1"},
						{"name", LabelEq, "kafka"},
					},
				},
				Pipeline: []PipelineStage{
					{
						LabelFilter: &LabelFilter{
							Paren: &LabelFilter{
								Pred: &LabelPredicate{
									Matcher: &LabelMatcher{Label: "service", Op: LabelEq, Value: "sus"},
								},
							},
						},
					},
					{
						LabelFilter: &LabelFilter{
							Pred: &LabelPredicate{
								Matcher: &LabelMatcher{Label: "service", Op: LabelEq, Value: "sus"},
							},
							Next: &NextLabelFilter{
								Op: ",",
								Filter: &LabelFilter{
									Pred: &LabelPredicate{
										Matcher: &LabelMatcher{Label: "request", Op: LabelNotEq, Value: "GET"},
									},
								},
							},
						},
					},
					{
						LabelFilter: &LabelFilter{
							Paren: &LabelFilter{
								Paren: &LabelFilter{
									Pred: &LabelPredicate{
										Matcher: &LabelMatcher{Label: "service", Op: LabelEq, Value: "sus"},
									},
								},
								Next: &NextLabelFilter{
									Op: "and",
									Filter: &LabelFilter{
										Pred: &LabelPredicate{
											Matcher: &LabelMatcher{Label: "request", Op: LabelNotEq, Value: "GET"},
										},
									},
								},
							},
						},
					},
					{
						LabelFilter: &LabelFilter{
							Pred: &LabelPredicate{
								Duration: &DurationFilter{Label: "duration", Op: FilterOpGte, Duration: "20ms"},
							},
							Next: &NextLabelFilter{
								Op: "or",
								Filter: &LabelFilter{
									Pred: &LabelPredicate{
										Bytes: &BytesFilter{Label: "size", Op: FilterOpEq, Bytes: "20kb"},
									},
									Next: &NextLabelFilter{
										Op: "and",
										Filter: &LabelFilter{
											Pred: &LabelPredicate{
												Matcher: &LabelMatcher{Label: "method", Op: LabelNotRe, Value: "2.."},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			false,
		},

		{"{", nil, true},
		{`{foo == "bar"}`, nil, true},
		{`{foo = "bar"} |`, nil, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Input:\n%s", tt.input)
				}
			}()

			got, err := logqlParser.ParseString("<test>", tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
