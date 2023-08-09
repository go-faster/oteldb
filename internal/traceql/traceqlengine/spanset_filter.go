package traceqlengine

import (
	"fmt"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

func buildSpansetExpr(expr traceql.SpansetExpr) (Processor, error) {
	switch expr := expr.(type) {
	case *traceql.SpansetFilter:
		return buildSpansetFilter(expr)
	case *traceql.BinarySpansetExpr:
		return buildBinarySpansetExpr(expr)
	default:
		return nil, errors.Errorf("unexpected spanset expression %T", expr)
	}
}

// SpansetFilter filters spansets by field expression.
type SpansetFilter struct {
	eval evaluater
}

func buildSpansetFilter(filter *traceql.SpansetFilter) (Processor, error) {
	eval, err := buildEvaluater(filter.Expr)
	if err != nil {
		return nil, errors.Wrap(err, "build spanset evaluater")
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

// BinarySpansetExpr merges two spansets.
type BinarySpansetExpr struct {
	left  Processor
	op    spansetOp
	right Processor
}

type spansetOp func(a, b []Spanset) ([]tracestorage.Span, error)

func buildBinarySpansetExpr(expr *traceql.BinarySpansetExpr) (Processor, error) {
	var op spansetOp
	switch expr.Op {
	case traceql.SpansetOpAnd:
		op = func(a, b []Spanset) ([]tracestorage.Span, error) {
			if len(a) == 0 || len(b) == 0 {
				return nil, nil
			}
			return mergeSpans(a, b), nil
		}
	case traceql.SpansetOpChild:
		op = func(a, b []Spanset) (result []tracestorage.Span, _ error) {
			if len(a) == 0 && len(b) == 0 {
				return nil, nil
			}
			if len(a) != 1 && len(b) != 1 {
				return nil, errors.New("can't find children spans of multiple spansets at once, try to use colaesce()")
			}
			return childSpans(a[0], b[0]), nil
		}
	case traceql.SpansetOpUnion:
		op = func(a, b []Spanset) ([]tracestorage.Span, error) {
			if len(a) == 0 && len(b) == 0 {
				return nil, nil
			}
			return mergeSpans(a, b), nil
		}
	case traceql.SpansetOpSibling:
		op = func(a, b []Spanset) (result []tracestorage.Span, _ error) {
			if len(a) == 0 && len(b) == 0 {
				return nil, nil
			}
			if len(a) != 1 && len(b) != 1 {
				return nil, errors.New("can't find siblings of multiple spansets at once, try to use colaesce()")
			}
			return siblingSpans(a[0], b[0]), nil
		}
	case traceql.SpansetOpDescendant:
		return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported spanset op %q", expr.Op)}
	default:
		return nil, errors.Errorf("unexpected spanset op %q", expr.Op)
	}

	left, err := buildSpansetExpr(expr.Left)
	if err != nil {
		return nil, errors.Wrap(err, "build left")
	}

	right, err := buildSpansetExpr(expr.Right)
	if err != nil {
		return nil, errors.Wrap(err, "build right")
	}

	return &BinarySpansetExpr{
		left:  left,
		op:    op,
		right: right,
	}, nil
}

type spanMerger struct {
	seen   map[otelstorage.SpanID]struct{}
	result []tracestorage.Span
}

func (s *spanMerger) Add(span tracestorage.Span) {
	spanID := span.SpanID
	if _, ok := s.seen[spanID]; ok {
		// Span already in result.
		return
	}

	if s.seen == nil {
		s.seen = map[otelstorage.SpanID]struct{}{}
	}
	s.seen[spanID] = struct{}{}
	s.result = append(s.result, span)
}

func (s *spanMerger) Result() []tracestorage.Span {
	return s.result
}

func mergeSpans(left, right []Spanset) []tracestorage.Span {
	// Left and right spasnets are guaranteed to have the same Trace ID.
	smaller := left
	bigger := right
	if len(right) < len(left) {
		smaller, bigger = right, left
	}

	m := spanMerger{}
	for _, ss := range smaller {
		for _, span := range ss.Spans {
			m.Add(span)
		}
	}

	for _, ss := range bigger {
		for _, span := range ss.Spans {
			m.Add(span)
		}
	}

	return m.result
}

func childSpans(left, right Spanset) []tracestorage.Span {
	leftIDs := map[otelstorage.SpanID]struct{}{}
	for _, span := range left.Spans {
		leftIDs[span.SpanID] = struct{}{}
	}

	m := spanMerger{}
	for _, span := range right.Spans {
		parentID := span.ParentSpanID
		if parentID.IsEmpty() {
			continue
		}

		if _, ok := leftIDs[parentID]; ok {
			m.Add(span)
		}
	}

	return m.Result()
}

func siblingSpans(left, right Spanset) []tracestorage.Span {
	leftParentIDs := map[otelstorage.SpanID]struct{}{}
	for _, span := range left.Spans {
		leftParentIDs[span.ParentSpanID] = struct{}{}
	}

	m := spanMerger{}
	for _, span := range right.Spans {
		if _, ok := leftParentIDs[span.ParentSpanID]; !ok {
			continue
		}
		m.Add(span)
	}

	return m.Result()
}

// Process implements Processor.
func (f *BinarySpansetExpr) Process(sets []Spanset) (output []Spanset, _ error) {
	for _, set := range sets {
		left, err := f.left.Process([]Spanset{set})
		if err != nil {
			return nil, errors.Wrap(err, "evaluate left")
		}

		right, err := f.right.Process([]Spanset{set})
		if err != nil {
			return nil, errors.Wrap(err, "evaluate right")
		}

		spans, err := f.op(left, right)
		if err != nil {
			return nil, errors.Wrap(err, "evaluate spanset op")
		}
		if len(spans) == 0 {
			continue
		}

		set.Spans = spans
		output = append(output, set)
	}

	return output, nil
}
