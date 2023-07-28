package logqlmetric

import (
	"fmt"
	"hash/fnv"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/go-faster/oteldb/internal/cmp"
	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

func testSampler(samples []SampledEntry) SampleSelector {
	return func(_ *logql.RangeAggregationExpr, _, _ time.Time) (iterators.Iterator[SampledEntry], error) {
		slices.SortStableFunc(samples, func(a, b SampledEntry) int {
			return cmp.Compare(a.Timestamp, b.Timestamp)
		})
		return iterators.Slice(samples), nil
	}
}

func evaluateQuery(t *testing.T, samples []SampledEntry, query string, params EvalParams, instant bool) lokiapi.QueryResponseData {
	t.Helper()

	defer func() {
		if r := recover(); r != nil || t.Failed() {
			t.Logf("Query: \n%s", query)
		}
	}()

	expr, err := logql.Parse(query, logql.ParseOptions{})
	require.NoError(t, err)

	require.Implements(t, (*logql.MetricExpr)(nil), expr)
	metricExpr := expr.(logql.MetricExpr)

	agg, err := Build(metricExpr, testSampler(samples), params)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, agg.Close())
	}()

	data, err := ReadStepResponse(
		agg,
		instant,
	)
	require.NoError(t, err)

	return data
}

func TestInstantAggregation(t *testing.T) {
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
			data := evaluateQuery(t, testSamples, tt.query, testParams, true)

			v, ok := data.GetVectorResult()
			require.True(t, ok)
			require.NotEmpty(t, v.Result)
			sample := v.Result[0]
			require.Equal(t, tt.expected, sample.Value.V)
		})
	}
}

func TestRangeAggregation(t *testing.T) {
	var (
		testParams = EvalParams{
			Start: time.Unix(1700000000, 0),
			End:   time.Unix(1700000010, 0),
			Step:  3 * time.Second,
		}
		testSamples = []SampledEntry{
			// Step 1.
			{Sample: 1, Timestamp: 1700000001_000000000, Set: &emptyLabels{}},
			{Sample: 2, Timestamp: 1700000002_000000000, Set: &emptyLabels{}},
			{Sample: 3, Timestamp: 1700000003_000000000, Set: &emptyLabels{}},

			// Step 2.
			{Sample: 4, Timestamp: 1700000004_000000000, Set: &emptyLabels{}},
			{Sample: 5, Timestamp: 1700000005_000000000, Set: &emptyLabels{}},
			{Sample: 6, Timestamp: 1700000006_000000000, Set: &emptyLabels{}},

			// Step 3.
			{Sample: 1, Timestamp: 1700000007_000000000, Set: &emptyLabels{}},
			{Sample: 2, Timestamp: 1700000008_000000000, Set: &emptyLabels{}},
			{Sample: 3, Timestamp: 1700000008_100000000, Set: &emptyLabels{}},
			{Sample: 4, Timestamp: 1700000009_000000000, Set: &emptyLabels{}},
		}
	)

	tests := []struct {
		query    string
		expected []string
	}{
		// Range aggregation.
		{`count_over_time({} [2s])`, []string{"3", "3", "4"}},
		{`rate({} [2s])`, []string{"1.5", "1.5", "2"}},            // count per log range interval
		{`rate({} | unwrap foo [2s])`, []string{"3", "7.5", "5"}}, // sum per log range interval
		{`bytes_over_time({} [2s])`, []string{"6", "15", "10"}},   // same as sum
		{`bytes_rate({} [2s])`, []string{"3", "7.5", "5"}},        // sum per log range interval
		{`avg_over_time({} | unwrap foo [2s])`, []string{"2", "5", "2.5"}},
		{`sum_over_time({} | unwrap foo [2s])`, []string{"6", "15", "10"}},
		{`min_over_time({} | unwrap foo [2s])`, []string{"1", "4", "1"}},
		{`max_over_time({} | unwrap foo [2s])`, []string{"3", "6", "4"}},
		{`first_over_time({} | unwrap foo [2s])`, []string{"1", "4", "1"}},
		{`last_over_time({} | unwrap foo [2s])`, []string{"3", "6", "4"}},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data := evaluateQuery(t, testSamples, tt.query, testParams, false)

			v, ok := data.GetMatrixResult()
			require.True(t, ok)
			require.NotEmpty(t, v.Result)
			group := v.Result[0].Values

			samples := make([]string, 0, len(group))
			for _, v := range group {
				samples = append(samples, v.V)
			}
			require.Equal(t, tt.expected, samples)
		})
	}
}

type testLabels map[string]string

// By returns new set of labels containing only given list of labels.
func (l testLabels) By(labels ...logql.Label) AggregatedLabels {
	return l.sub(labels, false)
}

// Without returns new set of labels without given list of labels.
func (l testLabels) Without(labels ...logql.Label) AggregatedLabels {
	return l.sub(labels, true)
}

func (l testLabels) sub(labels []logql.Label, without bool) AggregatedLabels {
	oldLabels := l

	if without {
		newLabels := maps.Clone(oldLabels)

		for _, label := range labels {
			delete(newLabels, string(label))
		}

		return newLabels
	}

	newLabels := testLabels{}
	for _, label := range labels {
		k := string(label)
		if v, ok := oldLabels[k]; ok {
			newLabels[k] = v
		}
	}
	return newLabels
}

// Key computes grouping key from set of labels.
func (l testLabels) Key() GroupingKey {
	h := fnv.New64()

	keys := maps.Keys(l)
	slices.Sort(keys)
	for _, k := range keys {
		v := l[k]
		io.WriteString(h, k)
		io.WriteString(h, v)
	}

	return h.Sum64()
}

// AsLokiAPI returns API structure for label set.
func (l testLabels) AsLokiAPI() lokiapi.LabelSet {
	return lokiapi.LabelSet(maps.Clone(l))
}

func TestGroupedAggregation(t *testing.T) {
	var (
		testParams = EvalParams{
			Start: time.Unix(1700000000, 0),
			End:   time.Unix(1700000016, 0),
			Step:  4 * time.Second,
		}
		testSamples = []SampledEntry{
			// Step 1.
			// foo=a
			{Sample: 1, Timestamp: 1700000001_000000000, Set: testLabels{"foo": "a", "method": "POST"}},
			{Sample: 2, Timestamp: 1700000002_000000000, Set: testLabels{"foo": "a", "method": "GET"}},
			{Sample: 3, Timestamp: 1700000003_000000000, Set: testLabels{"foo": "a", "method": "GET"}},
			// foo=b
			{Sample: 10, Timestamp: 1700000001_000000000, Set: testLabels{"foo": "b", "method": "GET"}},
			{Sample: 20, Timestamp: 1700000002_000000000, Set: testLabels{"foo": "b", "method": "POST"}},
			{Sample: 30, Timestamp: 1700000003_000000000, Set: testLabels{"foo": "b", "method": "GET"}},

			// Step 2.
			// foo=a
			{Sample: 5, Timestamp: 1700000005_000000000, Set: testLabels{"foo": "a", "method": "POST"}},
			{Sample: 6, Timestamp: 1700000006_000000000, Set: testLabels{"foo": "a", "method": "POST"}},
			{Sample: 7, Timestamp: 1700000007_000000000, Set: testLabels{"foo": "a", "method": "GET"}},
			// foo=b
			{Sample: 50, Timestamp: 1700000005_000000000, Set: testLabels{"foo": "b", "method": "GET"}},
			{Sample: 60, Timestamp: 1700000006_000000000, Set: testLabels{"foo": "b", "method": "GET"}},
			{Sample: 70, Timestamp: 1700000007_000000000, Set: testLabels{"foo": "b", "method": "POST"}},

			// Step 3.
			// foo=a
			{Sample: 10, Timestamp: 1700000009_000000000, Set: testLabels{"foo": "a", "method": "POST"}},
			{Sample: 20, Timestamp: 1700000010_000000000, Set: testLabels{"foo": "a", "method": "GET"}},
			{Sample: 30, Timestamp: 1700000011_000000000, Set: testLabels{"foo": "a", "method": "GET"}},
			// foo=b
			{Sample: 100, Timestamp: 1700000009_000000000, Set: testLabels{"foo": "b", "method": "GET"}},
			{Sample: 200, Timestamp: 1700000010_000000000, Set: testLabels{"foo": "b", "method": "POST"}},
			{Sample: 300, Timestamp: 1700000011_000000000, Set: testLabels{"foo": "b", "method": "GET"}},
		}
	)

	type series struct {
		labels map[string]string
		data   []string
	}
	tests := []struct {
		query    string
		expected []series
	}{
		// Range aggregation.
		{
			`count_over_time({} [4s])`,
			[]series{
				{
					map[string]string{
						"foo": "a", "method": "GET",
					},
					[]string{"2", "1", "2"},
				},
				{
					map[string]string{
						"foo": "a", "method": "POST",
					},
					[]string{"1", "2", "1"},
				},
				{
					map[string]string{
						"foo": "b", "method": "GET",
					},
					[]string{"2", "2", "2"},
				},
				{
					map[string]string{
						"foo": "b", "method": "POST",
					},
					[]string{"1", "1", "1"},
				},
			},
		},
		{
			`avg_over_time({} | unwrap _ [4s])`,
			[]series{
				{
					map[string]string{
						"foo": "a", "method": "GET",
					},
					[]string{"2.5", "7", "25"},
				},
				{
					map[string]string{
						"foo": "a", "method": "POST",
					},
					[]string{"1", "5.5", "10"},
				},
				{
					map[string]string{
						"foo": "b", "method": "GET",
					},
					[]string{"20", "55", "200"},
				},
				{
					map[string]string{
						"foo": "b", "method": "POST",
					},
					[]string{"20", "70", "200"},
				},
			},
		},
		{
			`max_over_time({} | unwrap _ [4s]) by (foo)`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"3", "7", "30"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"30", "70", "300"},
				},
			},
		},
		{
			`max_over_time({} | unwrap _ [4s]) without (method)`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"3", "7", "30"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"30", "70", "300"},
				},
			},
		},

		// Vector aggregation.
		{
			`sum by (foo) ( count_over_time({} [4s]) )`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"3", "3", "3"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"3", "3", "3"},
				},
			},
		},
		{
			`sum by (foo) ( count_over_time({} [4s]) ) * 2`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"6", "6", "6"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"6", "6", "6"},
				},
			},
		},
		{
			`sum without (method) ( count_over_time({} [4s]) )`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"3", "3", "3"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"3", "3", "3"},
				},
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data := evaluateQuery(t, testSamples, tt.query, testParams, false)

			v, ok := data.GetMatrixResult()
			require.True(t, ok)

			var got []series
			for _, from := range v.Result {
				to := series{labels: from.Metric.Value}
				for _, p := range from.Values {
					to.data = append(to.data, p.V)
				}
				got = append(got, to)
			}
			slices.SortFunc(got, func(a, b series) int {
				akey := a.labels["foo"] + a.labels["method"]
				bkey := b.labels["foo"] + b.labels["method"]
				return cmp.Compare(akey, bkey)
			})
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestKHeapAggregation(t *testing.T) {
	var (
		testParams = EvalParams{
			Start: time.Unix(1700000000, 0),
			End:   time.Unix(1700000006, 0),
			Step:  6 * time.Second,
		}
		testSamples = []SampledEntry{
			{Sample: 4, Timestamp: 1700000001_000000000, Set: testLabels{"key": "a", "sample": "1"}},
			{Sample: 5, Timestamp: 1700000002_000000000, Set: testLabels{"key": "a", "sample": "2"}},
			{Sample: 6, Timestamp: 1700000003_000000000, Set: testLabels{"key": "a", "sample": "3"}},
			{Sample: 3, Timestamp: 1700000004_000000000, Set: testLabels{"key": "a", "sample": "4"}},
			{Sample: 2, Timestamp: 1700000005_000000000, Set: testLabels{"key": "a", "sample": "5"}},
			{Sample: 1, Timestamp: 1700000006_000000000, Set: testLabels{"key": "a", "sample": "6"}},
		}
	)

	tests := []struct {
		query    string
		expected []string
	}{
		{`topk by (key) (2, sum_over_time({} | unwrap _ [6s]))`, []string{"6", "5"}},
		{`topk by (key) (3, sum_over_time({} | unwrap _ [6s]))`, []string{"6", "5", "4"}},
		{`bottomk by (key) (2, sum_over_time({} | unwrap _ [6s]))`, []string{"1", "2"}},
		{`bottomk by (key) (3, sum_over_time({} | unwrap _ [6s]))`, []string{"1", "2", "3"}},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data := evaluateQuery(t, testSamples, tt.query, testParams, false)

			v, ok := data.GetMatrixResult()
			require.True(t, ok)

			matrix := v.Result
			require.NotEmpty(t, matrix)

			var result []string
			for _, s := range matrix {
				values := s.Values
				require.Len(t, s.Values, 1)
				result = append(result, values[0].V)
			}
			require.ElementsMatch(t, tt.expected, result)
		})
	}
}
