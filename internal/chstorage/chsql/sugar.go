package chsql

import (
	"time"
)

// UnixNano returns time.Time as `toUnixTimestamp64Nano()`.
func UnixNano(t time.Time) Expr {
	return Function("toUnixTimestamp64Nano", Integer(t.UnixNano()))
}

// InTimeRange returns boolean expression to filter by [start:end].
func InTimeRange(column string, start, end time.Time) Expr {
	var (
		columnExpr = Ident(column)
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

// Contains returns boolean expression to filter strings containing needle.
func Contains(column, needle string) Expr {
	return Gt(
		PositionUTF8(Ident(column), String(needle)),
		Integer(0),
	)
}
