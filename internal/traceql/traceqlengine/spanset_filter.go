package traceqlengine

import (
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

// SpansetFilter filters spansets by field expression.
type SpansetFilter struct {
	eval evaluater
}

func buildSpansetFilter(filter *traceql.SpansetFilter) (Processor, error) {
	eval, err := buildEvaluater(filter.Expr)
	if err != nil {
		return nil, errors.Wrap(err, "build spanset filter")
	}
	return &SpansetFilter{eval: eval}, nil
}

// Process implements Processor.
func (f *SpansetFilter) Process(sets []Spanset) (result []Spanset, _ error) {
	var spans []tracestorage.Span
	for _, set := range sets {
		spans = spans[:0]
		ectx := set.evaluateCtx()
		for _, span := range set.Spans {
			if v := f.eval(span, ectx); v.Type == traceql.TypeBool && v.AsBool() {
				spans = append(spans, span)
			}
		}

		if len(spans) == 0 {
			continue
		}
		set.Spans = spans
		result = append(result, set)
	}
	return result, nil
}
