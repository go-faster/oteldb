package logqlengine

import (
	"go.uber.org/multierr"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type aggStep struct {
	ts      otelstorage.Timestamp
	samples []sample
}

type aggIterator interface {
	iterators.Iterator[aggStep]
}

type mergeAggIterator struct {
	left  aggIterator
	right aggIterator
	op    sampleBinOp
}

func newMergeAggIterator(
	left, right aggIterator,
	op sampleBinOp,
) *mergeAggIterator {
	return &mergeAggIterator{
		left:  left,
		right: right,
		op:    op,
	}
}

func (i *mergeAggIterator) Next(r *aggStep) bool {
	var left, right aggStep
	if !i.left.Next(&left) || !i.right.Next(&right) {
		return false
	}
	r.ts = left.ts

	getSamples := func(s aggStep) map[string]sample {
		r := make(map[string]sample, len(s.samples))
		for _, sample := range s.samples {
			r[sample.key] = sample
		}
		return r
	}
	leftSamples := getSamples(left)

	for _, rsample := range right.samples {
		lsample, ok := leftSamples[rsample.key]
		if !ok {
			continue
		}

		result, ok := i.op(lsample, rsample)
		if !ok {
			continue
		}

		r.samples = append(r.samples, result)
	}

	return true
}

func (i *mergeAggIterator) Err() error {
	return multierr.Append(
		i.left.Err(),
		i.right.Err(),
	)
}

func (i *mergeAggIterator) Close() error {
	return multierr.Append(
		i.left.Close(),
		i.right.Close(),
	)
}
