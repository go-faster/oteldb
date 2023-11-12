package metricsharding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestTimeMap(t *testing.T) {
	m := TimeMap[string, string]{}

	start := time.Date(2023, time.October, 10, 15, 0, 0, 0, time.UTC)
	m.Add(start, "a", "a")

	minuteAfter := start.Add(time.Minute)
	m.Add(minuteAfter, "a", "a")
	m.Add(minuteAfter, "b", "b")

	moreThanHourAfter := start.Add(time.Hour + time.Minute)
	m.Add(moreThanHourAfter, "a", "a")
	m.Add(moreThanHourAfter, "a", "a")
	m.Add(moreThanHourAfter, "b", "b")
	// Ensure default delta value is set.
	require.Equal(t, time.Hour, m.Delta)

	type pair struct {
		at  time.Time
		val string
	}
	var result []pair
	m.Each(func(t time.Time, vals []string) {
		for _, val := range vals {
			result = append(result, pair{
				at:  t,
				val: val,
			})
		}
	})
	slices.SortStableFunc(result, func(a, b pair) int {
		return a.at.Compare(b.at)
	})

	require.Equal(t,
		[]pair{
			{start, "a"},
			{start, "b"},
			{moreThanHourAfter.Truncate(m.Delta), "a"},
			{moreThanHourAfter.Truncate(m.Delta), "b"},
		},
		result,
	)
}
