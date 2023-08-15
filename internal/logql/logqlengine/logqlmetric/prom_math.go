package logqlmetric

import (
	"cmp"
	"math"
	"slices"
)

// Note: this file contains stats functions from Prometheus.

// quantile calculates the given quantile of a vector of samples.
//
// The Vector will be sorted.
// If 'values' has zero elements, NaN is returned.
// If q==NaN, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
func quantile(q float64, values []FPoint) float64 {
	if len(values) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	slices.SortFunc(values, func(a, b FPoint) int {
		if math.IsNaN(a.Value) {
			return -1
		}
		return cmp.Compare(a.Value, b.Value)
	})

	n := float64(len(values))
	// When the quantile lies between two samples,
	// we use a weighted average of the two samples.
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)
	lower := values[int(lowerIndex)]
	upper := values[int(upperIndex)]
	return lower.Value*(1-weight) + upper.Value*weight
}
