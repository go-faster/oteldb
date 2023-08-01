package traceql

import "github.com/go-faster/errors"

// TypedExpr is an interface for typed expression.
type TypedExpr interface {
	ValueType() StaticType
}

// CheckBinaryExpr typechecks binary expression.
func CheckBinaryExpr(left TypedExpr, op BinaryOp, right TypedExpr) error {
	lt := left.ValueType()
	rt := right.ValueType()
	if !op.CheckType(lt) {
		return errors.Errorf("can't apply op %q to %q", op, lt)
	}
	if !op.CheckType(rt) {
		return errors.Errorf("can't apply op %q to %q", op, rt)
	}
	if !lt.CheckOperand(rt) {
		return errors.Errorf("invalid operand types: %q and %q", lt, rt)
	}
	return nil
}

// CheckUnaryExpr typechecks unary expression.
func CheckUnaryExpr(op UnaryOp, expr TypedExpr) error {
	if t := expr.ValueType(); !op.CheckType(t) {
		return errors.Errorf("can't apply op %q to %q", op, t)
	}
	return nil
}
