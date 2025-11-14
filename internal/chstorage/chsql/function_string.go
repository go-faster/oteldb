package chsql

// Length returns `length(<arg>)` function call expression.
func Length(arg Expr) Expr {
	return Function("length", arg)
}

// Like returns `like(<haystack>, <pattern>)` function call expression.
func Like(haystack, pattern Expr) Expr {
	return Function("like", haystack, pattern)
}

// Position returns `position(<haystack>, <needle>)` function call expression.
func Position(haystack, needle Expr) Expr {
	return Function("position", haystack, needle)
}

// PositionUTF8 returns `positionUTF8(<haystack>, <needle>)` function call expression.
func PositionUTF8(haystack, needle Expr) Expr {
	return Function("positionUTF8", haystack, needle)
}

// Match returns `match(<haystack>, <pattern>)` function call expression.
func Match(haystack, pattern Expr) Expr {
	return Function("match", haystack, pattern)
}

// HasToken returns `hasToken(<haystack>, <token>)` function call expression.
func HasToken(haystack Expr, token string) Expr {
	return Function("hasToken", haystack, String(token))
}
