package traceqlengine

import (
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/traceql"
)

// SpansetFilter filters spansets by field expression.
type SpansetFilter struct {
	eval evaluater
}

func buildSpansetFilter(filter *traceql.SpansetFilter) (Processor, error) {
	eval, err := buildEvaluater(filter.Expr)
	if err != nil {
		return nil, errors.Wrap(err, "build Filter")
	}
	return &SpansetFilter{eval: eval}, nil
}

// Process implements Processor.
func (f *SpansetFilter) Process(sets []Spanset) ([]Spanset, error) {
	n := 0
	for _, set := range sets {
		if f.keep(set) {
			sets[n] = set
			n++
		}
	}
	sets = sets[:n]
	return sets, nil
}

func (f *SpansetFilter) keep(set Spanset) bool {
	for _, span := range set.Spans {
		v, ok := f.eval(span)
		if !ok {
			continue
		}
		if v.Type == traceql.TypeBool && v.AsBool() {
			return true
		}
	}
	return false
}
