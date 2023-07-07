package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/logql"
)

func Test_extractQueryConditions(t *testing.T) {
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
					{Label: "foo", Op: logql.OpRe, Value: "bar.+"},
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
					{Label: "foo", Op: logql.OpRe, Value: "bar.+"},
					{Label: "bar", Op: logql.OpNotRe, Value: "foo.+"},
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

			conds, err := extractQueryConditions(caps, tt.sel)
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
