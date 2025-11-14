package chsql

// Cast returns `CAST(<x>, <typ>)` function call expression.
func Cast(x Expr, typ string) Expr {
	return Function("CAST", x, String(typ))
}

// ToString returns `toString(<arg>)` function call expression.
func ToString(arg Expr) Expr {
	return Function("toString", arg)
}

// ToFloat64 returns `toFloat64(<arg>)` function call expression.
func ToFloat64(arg Expr) Expr {
	return Function("toFloat64", arg)
}
