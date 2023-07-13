package logqlengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
)

func TestRangeAggregation(t *testing.T) {
	tests := []struct {
		op       logql.RangeOp
		expected string
	}{
		{logql.RangeOpCount, "3"},
		{logql.RangeOpRate, "1.5"},    // count per log range interval
		{logql.RangeOpBytes, "6"},     // same as sum
		{logql.RangeOpBytesRate, "3"}, // sum per log range interval
		{logql.RangeOpAvg, "2"},
		{logql.RangeOpSum, "6"},
		{logql.RangeOpMin, "1"},
		{logql.RangeOpMax, "3"},
		{logql.RangeOpStdvar, "0.6666666666666666"},
		{logql.RangeOpStddev, "0.816496580927726"},
		{logql.RangeOpQuantile, "2.98"},
		{logql.RangeOpFirst, "1"},
		{logql.RangeOpLast, "3"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			samples := iterators.Slice([]sampledEntry{
				{sample: 1, entry: entry{ts: 1700000002_000000000}},
				{sample: 2, entry: entry{ts: 1700000003_000000000}},
				{sample: 3, entry: entry{ts: 1700000004_000000000}},
				// Would not be used.
				{sample: 10000, entry: entry{ts: 1700000005_000000000}},
			})

			param := 0.99
			expr := &logql.RangeAggregationExpr{
				Op:        tt.op,
				Range:     logql.LogRangeExpr{Range: 2 * time.Second},
				Parameter: &param,
			}
			params := EvalParams{
				Start: 1700000004_000000000,
				End:   1700000004_000000000,
				Step:  0,
				Limit: 1000,
			}
			require.True(t, params.IsInstant())

			var (
				start = params.Start.AsTime()
				end   = params.End.AsTime()
				step  = params.Step
			)
			agg, err := newRangeAggIterator(samples, expr, start, end, step)
			require.NoError(t, err)

			data, err := readRangeAggregation(
				agg,
				params.IsInstant(),
			)
			require.NoError(t, err)

			v, ok := data.GetVectorResult()
			require.True(t, ok)
			require.NotEmpty(t, v.Result)
			sample := v.Result[0]
			require.Equal(t, tt.expected, sample.Value.V)
		})
	}
}
