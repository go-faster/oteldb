package logqlmetric

import (
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
)

type vectorAggIterator struct {
	iter        iterators.Iterator[Step]
	agg         func() Aggregator
	grouper     grouperFunc
	groupLabels []logql.Label
}

// VectorAggregation creates new Vector aggregation step iterator.
func VectorAggregation(
	iter iterators.Iterator[Step],
	expr *logql.VectorAggregationExpr,
) (StepIterator, error) {
	agg, err := buildAggregator(expr)
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

	return &vectorAggIterator{
		iter:        iter,
		agg:         agg,
		grouper:     grouper,
		groupLabels: groupLabels,
	}, nil
}

func (i *vectorAggIterator) Next(r *Step) bool {
	var step Step
	if !i.iter.Next(&step) {
		return false
	}

	type group struct {
		metric AggregatedLabels
		agg    Aggregator
	}
	result := map[GroupingKey]*group{}

	for _, s := range step.Samples {
		metric := i.grouper(s.Set, i.groupLabels...)
		groupKey := metric.Key()

		g, ok := result[groupKey]
		if !ok {
			g = &group{
				metric: metric,
				agg:    i.agg(),
			}
			result[groupKey] = g
		}
		g.agg.Apply(s.Data)
	}

	r.Timestamp = step.Timestamp
	r.Samples = r.Samples[:0]
	for _, g := range result {
		r.Samples = append(r.Samples, Sample{
			Data: g.agg.Result(),
			Set:  g.metric,
		})
	}

	return true
}

func (i *vectorAggIterator) Err() error {
	return i.iter.Err()
}

func (i *vectorAggIterator) Close() error {
	return i.iter.Close()
}
