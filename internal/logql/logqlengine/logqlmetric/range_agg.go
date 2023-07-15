package logqlmetric

import (
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type grouperFunc = func(AggregatedLabels, ...logql.Label) AggregatedLabels

var nopGrouper = func(al AggregatedLabels, _ ...logql.Label) AggregatedLabels {
	return al
}

type rangeAggIterator struct {
	iter iterators.Iterator[SampledEntry]

	agg BatchAggregator
	// step state
	stepper stepper

	grouper     grouperFunc
	groupLabels []logql.Label
	// window state
	window   map[GroupingKey]Series
	interval time.Duration
	entry    SampledEntry
	// buffered whether last entry is buffered
	buffered bool
}

// RangeAggregation creates new range aggregation step iterator.
func RangeAggregation(
	iter iterators.Iterator[SampledEntry],
	expr *logql.RangeAggregationExpr,
	start, end time.Time,
	step time.Duration,
) (StepIterator, error) {
	if step == 0 {
		step = time.Second
	}

	aggtr, err := buildAggregator(expr)
	if err != nil {
		return nil, errors.Wrap(err, "build aggregator")
	}

	var (
		grouper     = nopGrouper
		groupLabels []logql.Label
	)
	if g := expr.Grouping; g != nil {
		groupLabels = g.Labels
		if g.Without {
			grouper = AggregatedLabels.Without
		} else {
			grouper = AggregatedLabels.By
		}
	}

	return &rangeAggIterator{
		iter: iter,

		agg:     aggtr,
		stepper: newStepper(start, end, step),

		grouper:     grouper,
		groupLabels: groupLabels,

		window:   map[GroupingKey]Series{},
		interval: expr.Range.Range,
	}, nil
}

func (i *rangeAggIterator) Next(r *Step) bool {
	current, ok := i.stepper.next()
	if !ok {
		return false
	}

	// Fill the window.
	windowStart := current.Add(-i.interval)
	windowEnd := current
	i.fillWindow(windowStart, windowEnd)

	// Aggregate the window.
	r.Timestamp = otelstorage.NewTimestampFromTime(current)
	r.Samples = r.Samples[:0]
	for _, s := range i.window {
		r.Samples = append(r.Samples, Sample{
			Data: i.agg.Aggregate(s.Data),
			Set:  s.Set,
		})
	}

	return true
}

func (i *rangeAggIterator) clearWindow(windowStart time.Time) {
	for key, s := range i.window {
		// Filter series data in place: timestamp should be >= windowStart.
		n := 0
		for _, p := range s.Data {
			if p.Timestamp.AsTime().Before(windowStart) {
				continue
			}
			s.Data[n] = p
			n++
		}
		s.Data = s.Data[:n]

		if len(s.Data) < 1 {
			// Delete empty series.
			delete(i.window, key)
		} else {
			i.window[key] = s
		}
	}
}

func (i *rangeAggIterator) fillWindow(windowStart, windowEnd time.Time) {
	i.clearWindow(windowStart)

	for {
		if !i.buffered {
			if !i.iter.Next(&i.entry) {
				return
			}
		} else {
			// Do not read next entry, use buffered
			i.buffered = false
		}

		e := i.entry
		switch ts := e.Timestamp.AsTime(); {
		case ts.After(windowEnd):
			// Entry is after the end of current window: buffer for the next window.
			i.buffered = true
			return
		case ts.Before(windowStart):
			// Entry is before the start of current window: just skip it.
			continue
		}

		metric := i.grouper(e.Set, i.groupLabels...)
		groupKey := metric.Key()

		ser, ok := i.window[groupKey]
		if !ok {
			ser.Set = metric
		}
		ser.Data = append(ser.Data, FPoint{
			Timestamp: e.Timestamp,
			Value:     e.Sample,
		})
		i.window[groupKey] = ser
	}
}

func (i *rangeAggIterator) Err() error {
	return i.iter.Err()
}

func (i *rangeAggIterator) Close() error {
	return i.iter.Close()
}
