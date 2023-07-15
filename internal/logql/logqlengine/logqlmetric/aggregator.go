package logqlmetric

import (
	"fmt"
	"math"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql"
)

// BatchAggregator is stateless batch aggregator.
type BatchAggregator interface {
	Aggregate(points []FPoint) float64
}

func buildAggregator(expr *logql.RangeAggregationExpr) (BatchAggregator, error) {
	qrange := expr.Range
	switch expr.Op {
	case logql.RangeOpCount:
		return &CountOverTime{}, nil
	case logql.RangeOpRate:
		if qrange.Unwrap == nil {
			return &Rate[CountOverTime]{selRange: qrange.Range.Seconds()}, nil
		}
		return &Rate[SumOverTime]{selRange: qrange.Range.Seconds()}, nil
	case logql.RangeOpRateCounter:
		// FIXME(tdakkota): implementation of rate_counter in Loki
		// 	is buggy, so keep it unimplemented.
		// return &rateCounter{selRange: qrange.Range}, nil
	case logql.RangeOpBytes:
		return &SumOverTime{}, nil
	case logql.RangeOpBytesRate:
		return &BytesRate{selRange: qrange.Range.Seconds()}, nil
	case logql.RangeOpAvg:
		return &AvgOverTime{}, nil
	case logql.RangeOpSum:
		return &SumOverTime{}, nil
	case logql.RangeOpMin:
		return &MinOverTime{}, nil
	case logql.RangeOpMax:
		return &MaxOverTime{}, nil
	case logql.RangeOpStdvar:
		return &StdvarOverTime{}, nil
	case logql.RangeOpStddev:
		return &StddevOverTime{}, nil
	case logql.RangeOpQuantile:
		p := expr.Parameter
		if p == nil {
			return nil, errors.Errorf("operation %q require a parameter", expr.Op)
		}
		return &QuantileOverTime{param: *p}, nil
	case logql.RangeOpFirst:
		return &FirstOverTime{}, nil
	case logql.RangeOpLast:
		return &LastOverTime{}, nil
	case logql.RangeOpAbsent:
	default:
		return nil, errors.Errorf("unexpected range operation %q", expr.Op)
	}
	return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported range operation %q", expr.Op)}
}

// CountOverTime implements `count_over_time` aggregation.
type CountOverTime struct{}

// Aggregate implements BatchAggregator.
func (CountOverTime) Aggregate(points []FPoint) float64 {
	return float64(len(points))
}

// Rate implements `rate` aggregation.
type Rate[A BatchAggregator] struct {
	preAgg   A
	selRange float64
}

// Aggregate implements BatchAggregator.
func (a Rate[A]) Aggregate(points []FPoint) float64 {
	return a.preAgg.Aggregate(points) / a.selRange
}

// BytesRate implements `bytes_rate` aggregation.
type BytesRate = Rate[SumOverTime]

// AvgOverTime implements `avg_over_time` aggregation.
type AvgOverTime struct{}

// Aggregate implements BatchAggregator.
func (AvgOverTime) Aggregate(points []FPoint) float64 {
	var cumAvg, count float64
	// https://en.wikipedia.org/wiki/Moving_average#Cumulative_average
	for _, p := range points {
		v := p.Value
		if math.IsInf(cumAvg, 0) {
			// If ca is Inf, do not try to overwrite it
			if math.IsInf(v, 0) {
				if (cumAvg > 0) == (v > 0) {
					continue
				}
				// unless new point is Inf with a different sign.
			} else {
				if !math.IsNaN(v) {
					continue
				}
				// unless new point is NaN
			}
		}
		// CA(n+1) = CA(n) + (x(n+1) - CA(n))/(n+1)
		count++
		cumAvg += (v - cumAvg) / count
	}
	return cumAvg
}

// SumOverTime implements `sum_over_time` aggregation.
type SumOverTime struct{}

// Aggregate implements BatchAggregator.
func (SumOverTime) Aggregate(points []FPoint) float64 {
	var sum float64
	for _, p := range points {
		sum += p.Value
	}
	return sum
}

// MinOverTime implements `min_over_time` aggregation.
type MinOverTime struct{}

// Aggregate implements BatchAggregator.
func (MinOverTime) Aggregate(points []FPoint) (min float64) {
	min = math.NaN()
	if len(points) > 0 {
		min = points[0].Value
	}
	for _, p := range points {
		v := p.Value
		if v < min || math.IsNaN(min) {
			min = v
		}
	}
	return min
}

// MaxOverTime implements `max_over_time` aggregation.
type MaxOverTime struct{}

// Aggregate implements BatchAggregator.
func (MaxOverTime) Aggregate(points []FPoint) (max float64) {
	max = math.NaN()
	if len(points) > 0 {
		max = points[0].Value
	}
	for _, p := range points {
		v := p.Value
		if v > max || math.IsNaN(max) {
			max = v
		}
	}
	return max
}

// StdvarOverTime implements `stdvar_over_time` aggregation.
type StdvarOverTime struct{}

// Aggregate implements BatchAggregator.
func (StdvarOverTime) Aggregate(points []FPoint) float64 {
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
	var m2, count, mean float64
	for _, p := range points {
		val := p.Value

		count++
		delta := val - mean
		mean += delta / count
		m2 += delta * (val - mean)
	}
	return m2 / count
}

// StddevOverTime implements `stddev_over_time` aggregation.
type StddevOverTime struct {
	stdvar StdvarOverTime
}

// Aggregate implements BatchAggregator.
func (a StddevOverTime) Aggregate(points []FPoint) float64 {
	variance := a.stdvar.Aggregate(points)
	return math.Sqrt(variance)
}

// QuantileOverTime implements `quantile_over_time` aggregation.
type QuantileOverTime struct {
	param float64
}

// Aggregate implements BatchAggregator.
func (a QuantileOverTime) Aggregate(points []FPoint) float64 {
	return quantile(a.param, points)
}

// FirstOverTime implements `first_over_time` aggregation.
type FirstOverTime struct{}

// Aggregate implements BatchAggregator.
func (FirstOverTime) Aggregate(points []FPoint) float64 {
	if len(points) == 0 {
		return 0
	}
	return points[0].Value
}

// LastOverTime implements `last_over_time` aggregation.
type LastOverTime struct{}

// Aggregate implements BatchAggregator.
func (LastOverTime) Aggregate(points []FPoint) (last float64) {
	if len(points) == 0 {
		return 0
	}
	return points[len(points)-1].Value
}
