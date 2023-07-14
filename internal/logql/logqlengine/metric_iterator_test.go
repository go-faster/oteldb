package logqlengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
)

func TestMergeAggregationIterator(t *testing.T) {
	tests := []struct {
		op       logql.RangeOp
		expected string
	}{
		{logql.RangeOpCount, "0"},
		{logql.RangeOpRate, "0"},      // count per log range interval
		{logql.RangeOpBytes, "0"},     // same as sum
		{logql.RangeOpBytesRate, "0"}, // sum per log range interval
		{logql.RangeOpAvg, "0"},
		{logql.RangeOpSum, "0"},
		{logql.RangeOpMin, "0"},
		{logql.RangeOpMax, "0"},
		{logql.RangeOpStdvar, "0"},
		{logql.RangeOpStddev, "0"},
		{logql.RangeOpQuantile, "0"},
		{logql.RangeOpFirst, "0"},
		{logql.RangeOpLast, "0"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
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

			getAggIter := func() *rangeAggIterator {
				samples := iterators.Slice([]sampledEntry{
					{sample: 1, entry: entry{ts: 1700000002_000000000}},
					{sample: 2, entry: entry{ts: 1700000003_000000000}},
					{sample: 3, entry: entry{ts: 1700000004_000000000}},
					// Would not be used.
					{sample: 10000, entry: entry{ts: 1700000005_000000000}},
				})

				var (
					start = params.Start.AsTime()
					end   = params.End.AsTime()
					step  = params.Step
				)

				iter, err := newRangeAggIterator(samples, expr, start, end, step)
				require.NoError(t, err)
				return iter
			}

			op, err := buildSampleBinOp(&logql.BinOpExpr{
				Op: logql.OpSub,
			})
			require.NoError(t, err)

			merge := newMergeAggIterator(getAggIter(), getAggIter(), op)

			data, err := readStepResponse(
				merge,
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
