package chsql

// ToUnixTimestamp64Nano returns `toUnixTimestamp64Nano(<arg>)` function call expression.
func ToUnixTimestamp64Nano(arg Expr) Expr {
	return Function("toUnixTimestamp64Nano", arg)
}

// ToStartOfDay returns `toStartOfHour(<arg>)` function call expression.
func ToStartOfHour(arg Expr) Expr {
	return Function("toStartOfHour", arg)
}
