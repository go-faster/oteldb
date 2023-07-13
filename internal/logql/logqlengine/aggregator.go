package logqlengine

import (
	"fmt"
	"math"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql"
)

type aggregator interface {
	Aggregate(points []fpoint) float64
}

func buildAggregator(expr *logql.RangeAggregationExpr) (aggregator, error) {
	qrange := expr.Range
	switch expr.Op {
	case logql.RangeOpCount:
		return &countOverTime{}, nil
	case logql.RangeOpRate:
		if qrange.Unwrap == nil {
			return &rate[countOverTime]{selRange: qrange.Range.Seconds()}, nil
		}
		return &rate[sumOverTime]{selRange: qrange.Range.Seconds()}, nil
	case logql.RangeOpRateCounter:
		// FIXME(tdakkota): implementation of rate_counter in Loki
		// 	is buggy, so keep it unimplemented.
		// return &rateCounter{selRange: qrange.Range}, nil
	case logql.RangeOpBytes:
		return &sumOverTime{}, nil
	case logql.RangeOpBytesRate:
		return &bytesRate{selRange: qrange.Range.Seconds()}, nil
	case logql.RangeOpAvg:
		return &avgOverTime{}, nil
	case logql.RangeOpSum:
		return &sumOverTime{}, nil
	case logql.RangeOpMin:
		return &minOverTime{}, nil
	case logql.RangeOpMax:
		return &maxOverTime{}, nil
	case logql.RangeOpStdvar:
		return &stdvarOverTime{}, nil
	case logql.RangeOpStddev:
		return &stddevOverTime{}, nil
	case logql.RangeOpQuantile:
		p := expr.Parameter
		if p == nil {
			return nil, errors.Errorf("operation %q require a parameter", expr.Op)
		}
		return &quantileOverTime{param: *p}, nil
	case logql.RangeOpFirst:
		return &firstOverTime{}, nil
	case logql.RangeOpLast:
		return &lastOverTime{}, nil
	case logql.RangeOpAbsent:
	}
	return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported range op %s", expr.Op)}
}

type countOverTime struct{}

func (countOverTime) Aggregate(points []fpoint) float64 {
	return float64(len(points))
}

type rate[A aggregator] struct {
	preAgg   A
	selRange float64
}

func (a rate[A]) Aggregate(points []fpoint) float64 {
	return a.preAgg.Aggregate(points) / a.selRange
}

type bytesRate = rate[sumOverTime]

type avgOverTime struct{}

func (avgOverTime) Aggregate(points []fpoint) float64 {
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

type sumOverTime struct{}

func (sumOverTime) Aggregate(points []fpoint) float64 {
	var sum float64
	for _, p := range points {
		sum += p.Value
	}
	return sum
}

type minOverTime struct{}

func (minOverTime) Aggregate(points []fpoint) (min float64) {
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

type maxOverTime struct{}

func (maxOverTime) Aggregate(points []fpoint) (max float64) {
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

type stdvarOverTime struct{}

func (stdvarOverTime) Aggregate(points []fpoint) float64 {
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

type stddevOverTime struct {
	stdvar stdvarOverTime
}

func (a stddevOverTime) Aggregate(points []fpoint) float64 {
	variance := a.stdvar.Aggregate(points)
	return math.Sqrt(variance)
}

type quantileOverTime struct {
	param float64
}

func (a quantileOverTime) Aggregate(points []fpoint) float64 {
	return quantile(a.param, points)
}

type firstOverTime struct{}

func (firstOverTime) Aggregate(points []fpoint) float64 {
	if len(points) == 0 {
		return 0
	}
	return points[0].Value
}

type lastOverTime struct{}

func (lastOverTime) Aggregate(points []fpoint) (last float64) {
	if len(points) == 0 {
		return 0
	}
	return points[len(points)-1].Value
}
