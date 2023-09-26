package logqlengine

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/logql"
)

func TestExtractLabelQueryConditions(t *testing.T) {
	tests := []struct {
		sel           logql.Selector
		labelCaps     []logql.BinOp
		wantPrefilter bool
		conds         SelectLogsParams
		wantErr       bool
	}{
		{
			logql.Selector{
				Matchers: []logql.LabelMatcher{
					{Label: "foo", Op: logql.OpEq, Value: "bar"},
					{Label: "bar", Op: logql.OpNotEq, Value: "foo"},
				},
			},
			[]logql.BinOp{logql.OpEq, logql.OpNotEq},
			false,
			SelectLogsParams{
				Labels: []logql.LabelMatcher{
					{Label: "foo", Op: logql.OpEq, Value: "bar"},
					{Label: "bar", Op: logql.OpNotEq, Value: "foo"},
				},
			},
			false,
		},
		{
			logql.Selector{
				Matchers: []logql.LabelMatcher{
					{Label: "foo", Op: logql.OpRe, Value: "bar.+", Re: regexp.MustCompile("bar.+")},
					{Label: "bar", Op: logql.OpNotEq, Value: "foo"},
				},
			},
			[]logql.BinOp{logql.OpEq, logql.OpNotEq},
			true,
			SelectLogsParams{
				Labels: []logql.LabelMatcher{
					{Label: "bar", Op: logql.OpNotEq, Value: "foo"},
				},
			},
			false,
		},
		{
			logql.Selector{
				Matchers: []logql.LabelMatcher{
					{Label: "foo", Op: logql.OpRe, Value: "bar.+", Re: regexp.MustCompile("bar.+")},
					{Label: "bar", Op: logql.OpNotRe, Value: "foo.+", Re: regexp.MustCompile("foo.+")},
				},
			},
			[]logql.BinOp{logql.OpEq, logql.OpNotEq},
			true,
			SelectLogsParams{},
			false,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var caps QuerierСapabilities
			caps.Label.Add(tt.labelCaps...)

			conds, err := extractQueryConditions(caps, tt.sel, nil)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.wantPrefilter {
				require.NotEqual(t, NopProcessor, conds.prefilter)
			} else {
				require.Equal(t, NopProcessor, conds.prefilter)
			}
			require.Equal(t, tt.conds, conds.params)
		})
	}
}

func TestExtractLineQueryConditions(t *testing.T) {
	tests := []struct {
		stages   []logql.PipelineStage
		lineCaps []logql.BinOp
		conds    SelectLogsParams
		wantErr  bool
	}{
		{
			[]logql.PipelineStage{
				&logql.DropLabelsExpr{},
				&logql.LineFilter{Op: logql.OpEq, Value: "first"},
				&logql.LineFilter{Op: logql.OpRe, Value: "regular.+", Re: regexp.MustCompile(`regular.+`)},
				&logql.DecolorizeExpr{},
				// These would not be off-loaded.
				&logql.LineFilter{Op: logql.OpEq, Value: "second"},
				&logql.LineFilter{Op: logql.OpRe, Value: "no+", Re: regexp.MustCompile(`no.+`)},
			},
			[]logql.BinOp{
				logql.OpEq,
				logql.OpRe,
			},
			SelectLogsParams{
				Line: []logql.LineFilter{
					{Op: logql.OpEq, Value: "first"},
					{Op: logql.OpRe, Value: "regular.+", Re: regexp.MustCompile(`regular.+`)},
				},
			},
			false,
		},
		{
			[]logql.PipelineStage{
				&logql.LineFilter{Op: logql.OpRe, Value: "a.+", Re: regexp.MustCompile(`a.+`)},
				&logql.DecolorizeExpr{},
				&logql.LineFilter{Op: logql.OpRe, Value: "b+", Re: regexp.MustCompile(`b.+`)},
			},
			[]logql.BinOp{
				logql.OpEq,
			},
			SelectLogsParams{},
			false,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var caps QuerierСapabilities
			caps.Line.Add(tt.lineCaps...)

			conds, err := extractQueryConditions(caps, logql.Selector{}, tt.stages)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, NopProcessor, conds.prefilter)
			require.Equal(t, tt.conds, conds.params)
		})
	}
}
