package chsql

import "time"

// UnixNano returns time.Time as `toUnixTimestamp64Nano()`.
func UnixNano(t time.Time) expr {
	return Function("toUnixTimestamp64Nano", Integer(t.UnixNano()))
}

// InTimeRange returns boolean expression to filter by [start:end].
func InTimeRange(column string, start, end time.Time) expr {
	columnExpr := Ident(column)

	return And(
		Gte(columnExpr, UnixNano(start)),
		Lte(columnExpr, UnixNano(end)),
	)
}

// Contains returns boolean expression to filter strings containing needle.
func Contains(column, needle string) expr {
	return Gt(
		PositionUTF8(Ident(column), String(needle)),
		Integer(0),
	)
}
