package logqlengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
)

func TestLiteralOpAggIterator(t *testing.T) {
	tests := []struct {
		op       logql.RangeOp
		expected string
	}{
		{logql.RangeOpCount, "6"},
		{logql.RangeOpRate, "3"},      // count per log range interval
		{logql.RangeOpBytes, "12"},    // same as sum
		{logql.RangeOpBytesRate, "6"}, // sum per log range interval
		{logql.RangeOpAvg, "4"},
		{logql.RangeOpSum, "12"},
		{logql.RangeOpMin, "2"},
		{logql.RangeOpMax, "6"},
		{logql.RangeOpStdvar, "1.3333333333333333"},
		{logql.RangeOpStddev, "1.632993161855452"},
		{logql.RangeOpQuantile, "5.96"},
		{logql.RangeOpFirst, "2"},
		{logql.RangeOpLast, "6"},
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

			op, err := buildSampleBinOp(&logql.BinOpExpr{
				Op: logql.OpMul,
			})
			require.NoError(t, err)

			lit := newLiteralOpAggIterator(agg, op, 2., false)
			require.NoError(t, err)

			data, err := readStepResponse(
				lit,
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
