package traceql

import (
	"fmt"
	"text/scanner"
)

// TypedExpr is an interface for typed expression.
type TypedExpr interface {
	ValueType() StaticType
}

// checkBinaryExpr typechecks binary expression.
func (p *parser) checkBinaryExpr(
	left TypedExpr,
	op BinaryOp,
	opPos scanner.Position,
	right TypedExpr,
	rightPos scanner.Position,
) error {
	lt := left.ValueType()
	rt := right.ValueType()
	if !op.CheckType(lt) {
		return &TypeError{
			Msg: fmt.Sprintf("binary operator %q not defined on %q", op, lt),
			Pos: opPos,
		}
	}
	if !op.CheckType(rt) {
		return &TypeError{
			Msg: fmt.Sprintf("binary operator %q not defined on %q", op, rt),
			Pos: opPos,
		}
	}
	if !lt.CheckOperand(rt) {
		return &TypeError{
			Msg: fmt.Sprintf("operand types mismatch: %q and %q", lt, rt),
			Pos: rightPos,
		}
	}
	return nil
}
