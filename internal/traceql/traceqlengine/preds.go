package traceqlengine

import "github.com/go-faster/oteldb/internal/traceql"

func extractPredicates(expr traceql.Expr, params EvalParams) SelectSpansetsParams {
	e := predExtractor{
		params: SelectSpansetsParams{
			Start:       params.Start,
			End:         params.End,
			MinDuration: params.MinDuration,
			MaxDuration: params.MaxDuration,
			Limit:       params.Limit,
		},
	}
	e.Walk(expr)
	return e.params
}

type predExtractor struct {
	params SelectSpansetsParams
}

func (p *predExtractor) Walk(expr traceql.Expr) {
	p.params.Op = traceql.SpansetOpAnd
	p.walk(expr)
}

func (p *predExtractor) walk(expr traceql.Expr) {
	switch expr := expr.(type) {
	case *traceql.SpansetPipeline:
		for _, stage := range expr.Pipeline {
			p.walkStage(stage)
		}
	case *traceql.BinaryExpr:
		p.params.Op = traceql.SpansetOpUnion
		p.walk(expr.Left)
		p.walk(expr.Right)
	}
}

func (p *predExtractor) walkStage(stage traceql.PipelineStage) {
	switch stage := stage.(type) {
	case *traceql.BinarySpansetExpr:
		p.params.Op = traceql.SpansetOpUnion
		p.walkStage(stage.Left)
		p.walkStage(stage.Right)
	case *traceql.SpansetFilter:
		p.walkField(stage.Expr, false)
	case *traceql.ScalarFilter:
		p.walkScalar(stage.Left)
		p.walkScalar(stage.Right)
	case *traceql.GroupOperation:
		p.walkField(stage.By, false)
	case *traceql.CoalesceOperation:
	case *traceql.SelectOperation:
		for _, expr := range stage.Args {
			p.walkField(expr, false)
		}
	}
}

func (p *predExtractor) walkField(expr traceql.FieldExpr, negate bool) {
	switch expr := expr.(type) {
	case *traceql.BinaryFieldExpr:
		op := expr.Op
		if negate {
			op = negateOp(op)
		}
		if op == traceql.OpOr {
			p.params.Op = traceql.SpansetOpUnion
		}

		if expr.Op.IsBoolean() {
			switch left := expr.Left.(type) {
			case *traceql.Attribute:
				if right, ok := expr.Right.(*traceql.Static); ok {
					p.params.Matchers = append(p.params.Matchers, SpanMatcher{
						Attribute: *left,
						Op:        op,
						Static:    *right,
					})
					return
				}
			case *traceql.Static:
				if right, ok := expr.Right.(*traceql.Attribute); ok {
					p.params.Matchers = append(p.params.Matchers, SpanMatcher{
						Attribute: *right,
						Op:        flipOp(op), // Flip operation because attribute on the right side.
						Static:    *left,
					})
					return
				}
			}
		}

		p.walkField(expr.Left, negate)
		p.walkField(expr.Right, negate)
	case *traceql.UnaryFieldExpr:
		if expr.Op == traceql.OpNot {
			negate = !negate
		}
		p.walkField(expr.Expr, negate)
	case *traceql.Attribute:
		p.params.Matchers = append(p.params.Matchers, SpanMatcher{
			Attribute: *expr,
		})
	}
}

func (p *predExtractor) walkScalar(expr traceql.ScalarExpr) {
	switch expr := expr.(type) {
	case *traceql.BinaryScalarExpr:
		p.walkScalar(expr.Left)
		p.walkScalar(expr.Right)
	case *traceql.AggregateScalarExpr:
		p.walkField(expr.Field, false)
	}
}

func negateOp(op traceql.BinaryOp) traceql.BinaryOp {
	switch op {
	case traceql.OpAnd:
		return traceql.OpOr
	case traceql.OpOr:
		return traceql.OpAnd
	case traceql.OpEq:
		return traceql.OpNotEq
	case traceql.OpNotEq:
		return traceql.OpEq
	case traceql.OpRe:
		return traceql.OpNotRe
	case traceql.OpNotRe:
		return traceql.OpRe
	case traceql.OpGt:
		return traceql.OpLte
	case traceql.OpGte:
		return traceql.OpLt
	case traceql.OpLt:
		return traceql.OpGte
	case traceql.OpLte:
		return traceql.OpGt
	default:
		return op
	}
}

func flipOp(op traceql.BinaryOp) traceql.BinaryOp {
	switch op {
	case traceql.OpGt:
		return traceql.OpLt
	case traceql.OpGte:
		return traceql.OpLte
	case traceql.OpLt:
		return traceql.OpGt
	case traceql.OpLte:
		return traceql.OpGte
	default:
		return op
	}
}
