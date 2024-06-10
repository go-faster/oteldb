package chsql

import (
	"time"
)

// UnixNano returns time.Time as unix nano timestamp.
func UnixNano(t time.Time) Expr {
	return Integer(t.UnixNano())
}

// InTimeRange returns boolean expression to filter by [start:end].
func InTimeRange(column string, start, end time.Time) Expr {
	var (
		columnExpr = ToUnixTimestamp64Nano(Ident(column))
		expr       Expr
	)
	if !start.IsZero() {
		expr = Gte(columnExpr, UnixNano(start))
	}
	if !end.IsZero() {
		endExpr := Lte(columnExpr, UnixNano(end))
		if expr.IsZero() {
			expr = endExpr
		} else {
			expr = And(expr, endExpr)
		}
	}
	if expr.IsZero() {
		expr = Bool(true)
	}
	return expr
}

// ColumnEq returns new `=` operation on column and literal.
func ColumnEq[V litValue](column string, right V) Expr {
	return binaryOp(Ident(column), "=", Value(right))
}

// Contains returns boolean expression to filter strings containing needle.
func Contains(column, needle string) Expr {
	return Gt(
		PositionUTF8(Ident(column), String(needle)),
		Integer(0),
	)
}

// JoinAnd joins given expressions using AND op.
//
//   - If len(args) == 0, returns `true` literal.
//   - If len(args) == 1, returns first argument.
//   - Otherwise, joins arguments with AND.
func JoinAnd(args ...Expr) Expr {
	switch len(args) {
	case 0:
		return Bool(true)
	case 1:
		return args[0]
	default:
		return joinBinaryOp("AND", args)
	}
}

// JoinOr joins given expressions using OR op.
//
//   - If len(args) == 0, returns `true` literal.
//   - If len(args) == 1, returns first argument.
//   - Otherwise, joins arguments with OR.
func JoinOr(args ...Expr) Expr {
	switch len(args) {
	case 0:
		return Bool(true)
	case 1:
		return args[0]
	default:
		return joinBinaryOp("OR", args)
	}
}
