package traceqlengine

import (
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type evaluateCtx struct {
	RootSpanName    string
	RootServiceName string
	TraceDuration   time.Duration
}

type (
	evaluater func(span tracestorage.Span, ctx evaluateCtx) traceql.Static
	binaryOp  func(a, b traceql.Static) traceql.Static
)

func buildEvaluater(expr traceql.FieldExpr) (evaluater, error) {
	switch expr := expr.(type) {
	case *traceql.BinaryFieldExpr:
		return buildBinaryEvaluater(expr.Left, expr.Op, expr.Right)
	case *traceql.UnaryFieldExpr:
		return buildUnaryEvaluater(expr.Op, expr.Expr)
	case *traceql.Static:
		cpy := *expr
		return func(tracestorage.Span, evaluateCtx) traceql.Static {
			return cpy
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
	var pattern string
	if op.IsRegex() {
		static, ok := right.(*traceql.Static)
		if !ok {
			return nil, errors.Errorf("unexpected pattern expression %T", right)
		}
		if static.Type != traceql.TypeString {
			return nil, errors.Errorf("expected string pattern, got %q", static.Type)
		}
		pattern = static.AsString()
	}

	opEval, err := buildBinaryOp(op, pattern)
	if err != nil {
		return nil, err
	}

	leftEval, err := buildEvaluater(left)
	if err != nil {
		return nil, err
	}

	rightEval, err := buildEvaluater(right)
	if err != nil {
		return nil, err
	}

	return func(span tracestorage.Span, ctx evaluateCtx) traceql.Static {
		left := leftEval(span, ctx)
		right := rightEval(span, ctx)
		return opEval(left, right)
	}, nil
}

func buildBinaryOp(op traceql.BinaryOp, pattern string) (binaryOp, error) {
	switch op {
	case traceql.OpAnd:
		return func(a, b traceql.Static) (r traceql.Static) {
			if a.Type != traceql.TypeBool || b.Type != traceql.TypeBool {
				r.SetBool(false)
			} else {
				r.SetBool(a.AsBool() && b.AsBool())
			}
			return r
		}, nil
	case traceql.OpOr:
		return func(a, b traceql.Static) (r traceql.Static) {
			if a.Type != traceql.TypeBool || b.Type != traceql.TypeBool {
				r.SetBool(false)
			} else {
				r.SetBool(a.AsBool() || b.AsBool())
			}
			return r
		}, nil
	case traceql.OpAdd:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(a.AsFloat() + b.AsFloat())
			return r
		}, nil
	case traceql.OpSub:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(a.AsFloat() - b.AsFloat())
			return r
		}, nil
	case traceql.OpMul:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(a.AsFloat() * b.AsFloat())
			return r
		}, nil
	case traceql.OpDiv:
		return func(a, b traceql.Static) (r traceql.Static) {
			dividend := a.AsFloat()
			// Checked division.
			if dividend == 0 {
				r.SetNumber(math.NaN())
			} else {
				r.SetNumber(dividend / b.AsFloat())
			}
			return r
		}, nil
	case traceql.OpMod:
		return func(a, b traceql.Static) (r traceql.Static) {
			dividend := a.AsFloat()
			// Checked modular division.
			if dividend == 0 {
				r.SetNumber(math.NaN())
			} else {
				r.SetNumber(math.Mod(dividend, b.AsFloat()))
			}
			return r
		}, nil
	case traceql.OpPow:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(math.Pow(a.AsFloat(), b.AsFloat()))
			return r
		}, nil
	case traceql.OpEq:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) == 0)
			return r
		}, nil
	case traceql.OpNotEq:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) != 0)
			return r
		}, nil
	case traceql.OpRe, traceql.OpNotRe:
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, errors.Wrapf(err, "compile regexp %q", pattern)
		}

		if op == traceql.OpRe {
			return func(a, _ traceql.Static) (r traceql.Static) {
				if a.Type != traceql.TypeString {
					r.SetBool(false)
				} else {
					r.SetBool(re.MatchString(a.AsString()))
				}
				return r
			}, nil
		}
		return func(a, _ traceql.Static) (r traceql.Static) {
			if a.Type != traceql.TypeString {
				r.SetBool(false)
			} else {
				r.SetBool(!re.MatchString(a.AsString()))
			}
			return r
		}, nil
	case traceql.OpGt:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) > 0)
			return r
		}, nil
	case traceql.OpGte:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) >= 0)
			return r
		}, nil
	case traceql.OpLt:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) < 0)
			return r
		}, nil
	case traceql.OpLte:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) <= 0)
			return r
		}, nil
	default:
		return nil, errors.Errorf("unexpected binary op %q", op)
	}
}

func buildUnaryEvaluater(op traceql.UnaryOp, expr traceql.FieldExpr) (evaluater, error) {
	sub, err := buildEvaluater(expr)
	if err != nil {
		return nil, err
	}
	switch op {
	case traceql.OpNeg:
		return func(span tracestorage.Span, ctx evaluateCtx) (r traceql.Static) {
			val := sub(span, ctx)
			if !val.Type.IsNumeric() {
				r.SetNil()
			} else {
				r.SetNumber(-val.AsFloat())
			}
			return r
		}, nil
	case traceql.OpNot:
		return func(span tracestorage.Span, ctx evaluateCtx) (r traceql.Static) {
			val := sub(span, ctx)
			if val.Type != traceql.TypeBool {
				r.SetNil()
			} else {
				r.SetBool(!val.AsBool())
			}
			return r
		}, nil
	default:
		return nil, errors.Errorf("unexpected unary op %q", op)
	}
}

func buildAttributeEvaluater(attr *traceql.Attribute) (evaluater, error) {
	switch attr.Prop {
	case traceql.SpanDuration:
		return func(span tracestorage.Span, _ evaluateCtx) (r traceql.Static) {
			end := span.End.AsTime()
			start := span.Start.AsTime()
			r.SetDuration(end.Sub(start))
			return r
		}, nil
	case traceql.SpanChildCount:
		// TODO(tdakkota): span child count
	case traceql.SpanName:
		return func(span tracestorage.Span, _ evaluateCtx) (r traceql.Static) {
			r.SetString(span.Name)
			return r
		}, nil
	case traceql.SpanStatus:
		return func(span tracestorage.Span, _ evaluateCtx) (r traceql.Static) {
			r.SetSpanStatus(ptrace.StatusCode(span.StatusCode))
			return r
		}, nil
	case traceql.SpanKind:
		return func(span tracestorage.Span, _ evaluateCtx) (r traceql.Static) {
			r.SetSpanKind(ptrace.SpanKind(span.Kind))
			return r
		}, nil
	case traceql.SpanParent:
		return func(span tracestorage.Span, _ evaluateCtx) (r traceql.Static) {
			if span.ParentSpanID.IsEmpty() {
				r.SetNil()
			} else {
				// Just set some non-nil value.
				r.SetBool(true)
			}
			return r
		}, nil
	case traceql.RootSpanName:
		return func(span tracestorage.Span, ctx evaluateCtx) (r traceql.Static) {
			r.SetString(ctx.RootSpanName)
			return r
		}, nil
	case traceql.RootServiceName:
		return func(span tracestorage.Span, ctx evaluateCtx) (r traceql.Static) {
			r.SetString(ctx.RootServiceName)
			return r
		}, nil
	case traceql.TraceDuration:
		return func(span tracestorage.Span, ctx evaluateCtx) (r traceql.Static) {
			r.SetDuration(ctx.TraceDuration)
			return r
		}, nil
	default:
		// SpanAttribute.
		if attr.Parent {
			// TODO(tdakkota): parent span attributes
			break
		}

		switch attr.Scope {
		case traceql.ScopeResource:
			return func(span tracestorage.Span, _ evaluateCtx) (r traceql.Static) {
				return evaluateAttr(
					attr.Name,
					span.ScopeAttrs,
					span.ResourceAttrs,
				)
			}, nil
		case traceql.ScopeSpan:
			return func(span tracestorage.Span, _ evaluateCtx) (r traceql.Static) {
				return evaluateAttr(
					attr.Name,
					span.Attrs,
				)
			}, nil
		default:
			return func(span tracestorage.Span, _ evaluateCtx) (r traceql.Static) {
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

func evaluateAttr(name string, attrs ...otelstorage.Attrs) (r traceql.Static) {
	for _, m := range attrs {
		if m.IsZero() {
			continue
		}
		if v, ok := m.AsMap().Get(name); ok && r.SetOTELValue(v) {
			return r
		}
	}
	r.SetNil()
	return r
}
