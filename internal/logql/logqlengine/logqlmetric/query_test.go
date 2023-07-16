package logqlmetric

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
)

var (
	testParams = EvalParams{
		Start: time.Unix(1700000004, 0),
		End:   time.Unix(1700000004, 0),
		Step:  0,
	}
	testSamples = []SampledEntry{
		{Sample: 1, Timestamp: 1700000002_000000000, Set: &emptyLabels{}},
		{Sample: 2, Timestamp: 1700000003_000000000, Set: &emptyLabels{}},
		{Sample: 3, Timestamp: 1700000004_000000000, Set: &emptyLabels{}},
		// Would not be used.
		{Sample: 10000, Timestamp: 1700000005_000000000, Set: &emptyLabels{}},
	}
)

func testSampler(samples []SampledEntry) SampleSelector {
	return func(logql.LogRangeExpr) (iterators.Iterator[SampledEntry], error) {
		return iterators.Slice(samples), nil
	}
}

func TestAggregation(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		// Range aggregation.
		{`count_over_time({} [2s])`, "3"},
		{`rate({} [2s])`, "1.5"},            // count per log range interval
		{`rate({} | unwrap foo [2s])`, "3"}, // sum per log range interval
		{`bytes_over_time({} [2s])`, "6"},   // same as sum
		{`bytes_rate({} [2s])`, "3"},        // sum per log range interval
		{`avg_over_time({} | unwrap foo [2s])`, "2"},
		{`sum_over_time({} | unwrap foo [2s])`, "6"},
		{`min_over_time({} | unwrap foo [2s])`, "1"},
		{`max_over_time({} | unwrap foo [2s])`, "3"},
		{`stdvar_over_time({} | unwrap foo [2s])`, "0.6666666666666666"},
		{`stddev_over_time({} | unwrap foo [2s])`, "0.816496580927726"},
		{`quantile_over_time(0.99, {} | unwrap foo [2s])`, "2.98"},
		{`first_over_time({} | unwrap foo [2s])`, "1"},
		{`last_over_time({} | unwrap foo [2s])`, "3"},
		// Vector aggregation.
		{`count(count_over_time({} [2s]))`, "1"},
		{`sum(count_over_time({} [2s]))`, "3"},
		{`avg(count_over_time({} [2s]))`, "3"},

		// Vector function.
		{`vector(1)`, "1"},
		{`vector(3.14)`, "3.14"},

		// Binary operation.
		// With literal.
		// Addition.
		{`vector(2) + 2`, "4"},
		// Subtraction.
		{`vector(2) - 2`, "0"},
		// Multiplication.
		{`vector(2) * 2`, "4"},
		{`2 * vector(2)`, "4"},
		// Division.
		{`vector(4) / 2`, "2"},
		{`vector(4) / 0`, "NaN"},
		{`2 / vector(4)`, "0.5"},
		{`2 / vector(0)`, "NaN"},
		// Modular division.
		{`vector(4) % 2`, "0"},
		{`vector(3) % 2`, "1"},
		{`vector(4) % 0`, "NaN"},
		{`4 % vector(2)`, "0"},
		{`3 % vector(2)`, "1"},
		{`2 % vector(0)`, "NaN"},
		// Exponentiation.
		{`vector(2) ^ 4`, "16"},
		{`2 ^ vector(3)`, "8"},
		// Comparison operations.
		{`vector(2) == 2`, "1"},
		{`vector(2) == 1`, "0"},
		{`vector(2) != 1`, "1"},
		{`vector(2) != 2`, "0"},
		{`vector(2) > 1`, "1"},
		{`vector(2) > 2`, "0"},
		{`vector(2) >= 1`, "1"},
		{`vector(2) >= 2`, "1"},
		{`vector(2) >= 3`, "0"},
		{`vector(2) < 3`, "1"},
		{`vector(2) < 2`, "0"},
		{`vector(2) <= 3`, "1"},
		{`vector(2) <= 2`, "1"},
		{`vector(2) <= 1`, "0"},

		// Operations with range.
		{`count_over_time({} [2s]) * 2`, "6"},
		{`2 * count_over_time({} [2s])`, "6"},
		// Between vectors.
		{`vector(2) * vector(2)`, "4"},
		// Between ranges.
		{`count_over_time({} [2s]) - count_over_time({} [2s])`, "0"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil || t.Failed() {
					t.Logf("Query: \n%s", tt.query)
				}
			}()

			expr, err := logql.Parse(tt.query, logql.ParseOptions{})
			require.NoError(t, err)

			require.Implements(t, (*logql.MetricExpr)(nil), expr)
			metricExpr := expr.(logql.MetricExpr)

			agg, err := Build(metricExpr, testSampler(testSamples), testParams)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, agg.Close())
			}()

			data, err := ReadStepResponse(
				agg,
				true,
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
