package logqlengine

import (
	"math"
	"time"

	"golang.org/x/exp/slices"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Note: this file contains stats functions from Prometheus.

// extrapolatedRate is a utility function for rate/increase/delta.
// It calculates the rate (allowing for counter resets if isCounter is true),
// extrapolates if the first/last sample is close to the boundary, and returns
// the result as either per-second (if isRate is true) or overall.
func extrapolatedRate(samples []fpoint, selRange time.Duration, isCounter, isRate bool) float64 {
	durationMilliseconds := func(d time.Duration) otelstorage.Timestamp {
		return otelstorage.Timestamp(d / (time.Millisecond / time.Nanosecond))
	}

	// No sense in trying to compute a rate without at least two points. Drop
	// this Vector element.
	if len(samples) < 2 {
		return 0
	}
	var (
		rangeStart = samples[0].Timestamp - durationMilliseconds(selRange)
		rangeEnd   = samples[len(samples)-1].Timestamp
	)

	resultValue := samples[len(samples)-1].Value - samples[0].Value
	if isCounter {
		var lastValue float64
		for _, sample := range samples {
			if sample.Value < lastValue {
				resultValue += lastValue
			}
			lastValue = sample.Value
		}
	}

	// Duration between first/last samples and boundary of range.
	durationToStart := float64(samples[0].Timestamp-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-samples[len(samples)-1].Timestamp) / 1000

	sampledInterval := float64(samples[len(samples)-1].Timestamp-samples[0].Timestamp) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(len(samples)-1)

	if isCounter && resultValue > 0 && samples[0].Value >= 0 {
		// Counters cannot be negative. If we have any slope at
		// all (i.e. resultValue went up), we can extrapolate
		// the zero point of the counter. If the duration to the
		// zero point is shorter than the durationToStart, we
		// take the zero point as the start of the series,
		// thereby avoiding extrapolation to negative counter
		// values.
		durationToZero := sampledInterval * (samples[0].Value / resultValue)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	// If the first/last samples are close to the boundaries of the range,
	// extrapolate the result. This is as we expect that another sample
	// will exist given the spacing between samples we've seen thus far,
	// with an allowance for noise.
	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart < extrapolationThreshold {
		extrapolateToInterval += durationToStart
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	if durationToEnd < extrapolationThreshold {
		extrapolateToInterval += durationToEnd
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	resultValue *= (extrapolateToInterval / sampledInterval)
	if isRate {
		seconds := selRange.Seconds()
		resultValue /= seconds
	}

	return resultValue
}

// quantile calculates the given quantile of a vector of samples.
//
// The Vector will be sorted.
// If 'values' has zero elements, NaN is returned.
// If q==NaN, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
func quantile(q float64, values []fpoint) float64 {
	if len(values) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	slices.SortFunc(values, func(a, b fpoint) bool {
		if math.IsNaN(a.Value) {
			return true
		}
		return a.Value < b.Value
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
