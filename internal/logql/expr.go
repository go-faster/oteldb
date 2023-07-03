package logql

import (
	"math"

	"github.com/go-faster/errors"
)

// Expr is a root LogQL expression.
type Expr interface {
	expr()
}

func (*ParenExpr) expr() {}
func (*BinOpExpr) expr() {}

// ParenExpr is parenthesized Expr.
type ParenExpr struct {
	X Expr
}

// BinOpExpr defines a binary operation between two Expr.
type BinOpExpr struct {
	Left     Expr
	Op       BinOp
	Modifier BinOpModifier
	Right    Expr
}

// BinOpModifier defines BinOpExpr modifier.
//
// FIXME(tdakkota): this feature is not well documented.
type BinOpModifier struct {
	Op         string // on, ignoring
	OpLabels   []Label
	Group      string // "", "left", "right"
	Include    []Label
	ReturnBool bool
}

// UnparenExpr recursively extracts expression from parentheses.
func UnparenExpr(e Expr) Expr {
	p, ok := e.(*ParenExpr)
	if !ok {
		return e
	}
	return UnparenExpr(p.X)
}

// ReduceBinOp recursively precomputes literal expression.
//
// If expression is not constant, returns nil.
func ReduceBinOp(b *BinOpExpr) (_ *LiteralExpr, err error) {
	left := UnparenExpr(b.Left)
	right := UnparenExpr(b.Right)

	if sub, ok := left.(*BinOpExpr); ok {
		reduced, err := ReduceBinOp(sub)
		if reduced == nil || err != nil {
			return nil, err
		}
		left = reduced
	}
	if sub, ok := right.(*BinOpExpr); ok {
		reduced, err := ReduceBinOp(sub)
		if reduced == nil || err != nil {
			return nil, err
		}
		right = reduced
	}

	lv, ok := left.(*LiteralExpr)
	if !ok {
		return nil, nil
	}
	rv, ok := right.(*LiteralExpr)
	if !ok {
		return nil, nil
	}

	var r float64
	switch b.Op {
	case OpAdd:
		r = lv.Value + rv.Value
	case OpSub:
		r = lv.Value - rv.Value
	case OpMul:
		r = lv.Value * rv.Value
	case OpDiv:
		if rv.Value == 0 {
			r = math.NaN()
		} else {
			r = lv.Value / rv.Value
		}
	case OpMod:
		if rv.Value == 0 {
			r = math.NaN()
		} else {
			r = math.Mod(lv.Value, rv.Value)
		}
	case OpPow:
		r = math.Pow(lv.Value, rv.Value)
	case OpEq:
		if lv.Value == rv.Value {
			r = 1.
		}
	case OpNotEq:
		if lv.Value != rv.Value {
			r = 1.
		}
	case OpGt:
		if lv.Value > rv.Value {
			r = 1.
		}
	case OpGte:
		if lv.Value >= rv.Value {
			r = 1.
		}
	case OpLt:
		if lv.Value < rv.Value {
			r = 1.
		}
	case OpLte:
		if lv.Value <= rv.Value {
			r = 1.
		}
	default:
		return nil, errors.Errorf("unexpected operation %s", b.Op)
	}

	return &LiteralExpr{Value: r}, nil
}
