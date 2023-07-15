package logqlmetric

import (
	"github.com/go-faster/errors"
	"go.uber.org/multierr"

	"github.com/go-faster/oteldb/internal/logql"
)

// BinOp returns new step iterator performing binary operation between two iterators.
func BinOp(
	left, right StepIterator,
	expr *logql.BinOpExpr,
) (StepIterator, error) {
	switch expr.Op {
	case logql.OpAnd, logql.OpOr, logql.OpUnless:
		return nil, &UnsupportedError{Msg: "binary set operations are unsupported yet"}
	}
	if m := expr.Modifier; m.Op != "" || len(m.OpLabels) > 0 || m.Group != "" || len(m.Include) > 0 {
		return nil, &UnsupportedError{Msg: "binary operation modifiers are unsupported yet"}
	}

	op, err := buildSampleBinOp(expr)
	if err != nil {
		return nil, errors.Wrap(err, "build binary sample operation")
	}

	return &binOpIterator{
		left:  left,
		right: right,
		op:    op,
	}, nil
}

type binOpIterator struct {
	left  StepIterator
	right StepIterator
	op    SampleOp
}

func (i *binOpIterator) Next(r *Step) bool {
	var left, right Step
	if !i.left.Next(&left) || !i.right.Next(&right) {
		return false
	}
	r.Timestamp = left.Timestamp

	getSamples := func(s Step) map[GroupingKey]Sample {
		r := make(map[GroupingKey]Sample, len(s.Samples))
		for _, sample := range s.Samples {
			key := sample.Set.Key()
			r[key] = sample
		}
		return r
	}
	leftSamples := getSamples(left)

	for _, rsample := range right.Samples {
		key := rsample.Set.Key()
		lsample, ok := leftSamples[key]
		if !ok {
			continue
		}

		result, ok := i.op(lsample, rsample)
		if !ok {
			continue
		}

		r.Samples = append(r.Samples, result)
	}

	return true
}

func (i *binOpIterator) Err() error {
	return multierr.Append(
		i.left.Err(),
		i.right.Err(),
	)
}

func (i *binOpIterator) Close() error {
	return multierr.Append(
		i.left.Close(),
		i.right.Close(),
	)
}

type literalBinOpIterator struct {
	iter  StepIterator
	op    SampleOp
	value float64
	// left whether on which side literal is
	left bool
}

// LiteralBinOp returns new step iterator performing binary operation with literal.
func LiteralBinOp(
	iter StepIterator,
	expr *logql.BinOpExpr,
	value float64,
	left bool,
) (StepIterator, error) {
	op, err := buildSampleBinOp(expr)
	if err != nil {
		return nil, errors.Wrap(err, "build binary sample operation")
	}
	return &literalBinOpIterator{
		iter:  iter,
		op:    op,
		value: value,
		left:  left,
	}, nil
}

func (i *literalBinOpIterator) Next(r *Step) bool {
	if !i.iter.Next(r) {
		return false
	}

	n := 0
	for _, agg := range r.Samples {
		literal := Sample{
			Data: i.value,
			Set:  agg.Set,
		}

		var left, right Sample
		if i.left {
			left, right = literal, agg
		} else {
			left, right = agg, literal
		}

		if val, ok := i.op(left, right); ok {
			r.Samples[n] = val
			n++
		}
	}
	r.Samples = r.Samples[:n]

	return true
}

func (i *literalBinOpIterator) Err() error {
	return i.iter.Err()
}

func (i *literalBinOpIterator) Close() error {
	return i.iter.Close()
}
