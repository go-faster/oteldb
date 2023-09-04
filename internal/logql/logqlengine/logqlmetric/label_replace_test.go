package logqlmetric

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
)

func TestLabelReplace(t *testing.T) {
	tests := []struct {
		steps  []Step
		expr   *logql.LabelReplaceExpr
		expect []Sample
	}{
		{
			[]Step{
				{
					Timestamp: 1,
					Samples: []Sample{
						{Data: 1, Set: testLabels{"sample": "1"}},
						{Data: 2, Set: testLabels{"sample": "2"}},
						{Data: 3, Set: testLabels{"sample": "3"}},
					},
				},
			},
			&logql.LabelReplaceExpr{
				DstLabel:    "dst_label",
				Replacement: "$digit th",
				SrcLabel:    "sample",
				Re:          regexp.MustCompile(`(?P<digit>\d+)`),
			},
			[]Sample{
				{Data: 1, Set: testLabels{"sample": "1", "dst_label": "1 th"}},
				{Data: 2, Set: testLabels{"sample": "2", "dst_label": "2 th"}},
				{Data: 3, Set: testLabels{"sample": "3", "dst_label": "3 th"}},
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			steps := iterators.Slice(tt.steps)

			iter, err := LabelReplace(steps, tt.expr)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, iter.Close())
			}()

			var result Step
			require.True(t, iter.Next(&result))
			require.Equal(t, tt.steps[0].Timestamp, result.Timestamp)
			require.Equal(t, tt.expect, result.Samples)

			require.False(t, iter.Next(&result))
			require.NoError(t, iter.Err())
		})
	}
}
