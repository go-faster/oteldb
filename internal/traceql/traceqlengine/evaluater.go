package traceqlengine

import (
	"fmt"
	"math"
	"regexp"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type evaluater func(span tracestorage.Span) (traceql.Static, bool)

func buildEvaluater(expr traceql.FieldExpr) (evaluater, error) {
	switch expr := expr.(type) {
	case *traceql.BinaryFieldExpr:
		return buildBinaryEvaluater(expr.Left, expr.Op, expr.Right)
	case *traceql.UnaryFieldExpr:
		return buildUnaryEvaluater(expr.Op, expr.Expr)
	case *traceql.Static:
		cpy := *expr
		return func(span tracestorage.Span) (traceql.Static, bool) {
			return cpy, true
		}, nil
	case *traceql.Attribute:
		return buildAttributeEvaluater(expr)
	default:
		return nil, errors.Errorf("unexpected expression %T", expr)
	}
}

func buildBinaryEvaluater(
	left traceql.FieldExpr,
	op traceql.BinaryOp,
	right traceql.FieldExpr,
) (evaluater, error) {
	var opEval func(a, b traceql.Static) traceql.Static
	switch op {
	case traceql.OpAnd:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			if a.Type != traceql.TypeBool || b.Type != traceql.TypeBool {
				r.SetBool(false)
			} else {
				r.SetBool(a.AsBool() && b.AsBool())
			}
			return r
		}
	case traceql.OpOr:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			if a.Type != traceql.TypeBool || b.Type != traceql.TypeBool {
				r.SetBool(false)
			} else {
				r.SetBool(a.AsBool() || b.AsBool())
			}
			return r
		}
	case traceql.OpAdd:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(a.AsFloat() + b.AsFloat())
			return r
		}
	case traceql.OpSub:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(a.AsFloat() - b.AsFloat())
			return r
		}
	case traceql.OpMul:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(a.AsFloat() * b.AsFloat())
			return r
		}
	case traceql.OpDiv:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			divider := b.AsFloat()
			// Checked division.
			if divider == 0 {
				r.SetNumber(math.NaN())
			} else {
				r.SetNumber(a.AsFloat() / divider)
			}
			return r
		}
	case traceql.OpMod:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(math.Mod(a.AsFloat(), b.AsFloat()))
			return r
		}
	case traceql.OpPow:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(math.Pow(a.AsFloat(), b.AsFloat()))
			return r
		}
	case traceql.OpEq:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) == 0)
			return r
		}
	case traceql.OpNotEq:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) != 0)
			return r
		}
	case traceql.OpRe, traceql.OpNotRe:
		static, ok := right.(*traceql.Static)
		if !ok {
			return nil, errors.Errorf("unexpected pattern expression %T", right)
		}
		if static.Type != traceql.TypeString {
			return nil, errors.Errorf("expected string pattern, got %q", static.Type)
		}
		pattern := static.AsString()

		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, errors.Wrapf(err, "compile regexp %q", pattern)
		}

		if op == traceql.OpRe {
			opEval = func(a, _ traceql.Static) (r traceql.Static) {
				if a.Type != traceql.TypeString {
					r.SetBool(false)
				} else {
					r.SetBool(re.MatchString(a.AsString()))
				}
				return r
			}
		} else {
			opEval = func(a, _ traceql.Static) (r traceql.Static) {
				if a.Type != traceql.TypeString {
					r.SetBool(false)
				} else {
					r.SetBool(!re.MatchString(a.AsString()))
				}
				return r
			}
		}
	case traceql.OpGt:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) > 0)
			return r
		}
	case traceql.OpGte:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) >= 0)
			return r
		}
	case traceql.OpLt:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) < 0)
			return r
		}
	case traceql.OpLte:
		opEval = func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) <= 0)
			return r
		}
	default:
		return nil, errors.Errorf("unexpected binary op %q", op)
	}

	leftEval, err := buildEvaluater(left)
	if err != nil {
		return nil, err
	}

	rightEval, err := buildEvaluater(right)
	if err != nil {
		return nil, err
	}

	return func(span tracestorage.Span) (r traceql.Static, _ bool) {
		left, ok := leftEval(span)
		if !ok {
			return r, false
		}

		right, ok := rightEval(span)
		if !ok {
			return r, false
		}

		return opEval(left, right), true
	}, nil
}

func buildUnaryEvaluater(op traceql.UnaryOp, expr traceql.FieldExpr) (evaluater, error) {
	sub, err := buildEvaluater(expr)
	if err != nil {
		return nil, err
	}
	switch op {
	case traceql.OpNeg:
		return func(span tracestorage.Span) (r traceql.Static, _ bool) {
			val, ok := sub(span)
			if !ok {
				return r, false
			}
			r.SetNumber(-val.AsFloat())
			return r, true
		}, nil
	case traceql.OpNot:
		return func(span tracestorage.Span) (r traceql.Static, _ bool) {
			val, ok := sub(span)
			if !ok || val.Type != traceql.TypeBool {
				return r, false
			}
			r.SetBool(!val.AsBool())
			return r, true
		}, nil
	default:
		return nil, errors.Errorf("unexpected unary op %q", op)
	}
}

func buildAttributeEvaluater(attr *traceql.Attribute) (evaluater, error) {
	switch attr.Prop {
	case traceql.SpanDuration:
		return func(span tracestorage.Span) (r traceql.Static, _ bool) {
			end := span.End.AsTime()
			start := span.Start.AsTime()
			r.SetDuration(end.Sub(start))
			return r, true
		}, nil
	case traceql.SpanChildCount:
		// TODO(tdakkota): span child count
	case traceql.SpanName:
		return func(span tracestorage.Span) (r traceql.Static, _ bool) {
			r.SetString(span.Name)
			return r, true
		}, nil
	case traceql.SpanStatus:
		return func(span tracestorage.Span) (r traceql.Static, _ bool) {
			r.SetSpanStatus(ptrace.StatusCode(span.StatusCode))
			return r, true
		}, nil

	case traceql.SpanKind:
		return func(span tracestorage.Span) (r traceql.Static, _ bool) {
			r.SetSpanKind(ptrace.SpanKind(span.Kind))
			return r, true
		}, nil
	case traceql.SpanParent:
		return func(span tracestorage.Span) (r traceql.Static, _ bool) {
			if span.ParentSpanID.IsEmpty() {
				r.SetNil()
			} else {
				// Just set some non-nil value.
				r.SetBool(true)
			}
			return r, true
		}, nil
	case traceql.RootSpanName:
		// TODO(tdakkota): root span attributes
	case traceql.RootServiceName:
		// TODO(tdakkota): root span attributes
	case traceql.TraceDuration:
		// TODO(tdakkota): trace attributes
	default:
		// SpanAttribute.
		if attr.Parent {
			// TODO(tdakkota): parent span attributes
			break
		}

		evaluateAttr := func(name string, attrs ...otelstorage.Attrs) (r traceql.Static, _ bool) {
			for _, m := range attrs {
				if m.IsZero() {
					continue
				}
				if v, ok := m.AsMap().Get(name); ok && r.SetOTELValue(v) {
					return r, true
				}
			}
			return r, false
		}
		switch attr.Scope {
		case traceql.ScopeResource:
			return func(span tracestorage.Span) (r traceql.Static, _ bool) {
				return evaluateAttr(
					attr.Name,
					span.ScopeAttrs,
					span.ResourceAttrs,
				)
			}, nil
		case traceql.ScopeSpan:
			return func(span tracestorage.Span) (r traceql.Static, _ bool) {
				return evaluateAttr(
					attr.Name,
					span.Attrs,
				)
			}, nil
		default:
			return func(span tracestorage.Span) (r traceql.Static, _ bool) {
				return evaluateAttr(
					attr.Name,
					span.Attrs,
					span.ScopeAttrs,
					span.ResourceAttrs,
				)
			}, nil
		}
	}
	return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported attribute %q", attr)}
}
