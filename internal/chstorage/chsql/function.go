package chsql

// Function reutrns function call expression.
func Function(name string, args ...expr) expr {
	return expr{typ: exprFunction, tok: name, args: args}
}

// JSONExtract returns `JSONExtract(<from>, <typ>)` function call expression.
func JSONExtract(from expr, typ string) expr {
	return Function("JSONExtract", from, String(typ))
}

// JSONExtractField returns `JSONExtract(<from>, <field>, <typ>)` function call expression.
func JSONExtractField(from expr, field, typ string) expr {
	return Function("JSONExtract", from, String(field), String(typ))
}

// Hex returns `hex(<arg>)` function call expression.
func Hex(arg expr) expr {
	return Function("hex", arg)
}

// Unhex returns `unhex(<arg>)` function call expression.
func Unhex(arg expr) expr {
	return Function("unhex", arg)
}

// PositionUTF8 returns `positionUTF8(<haystack>, <needle>)` function call expression.
func PositionUTF8(haystack, needle expr) expr {
	return Function("positionUTF8", haystack, needle)
}

// Match returns `match(<haystack>, <pattern>)` function call expression.
func Match(haystack, pattern expr) expr {
	return Function("match", haystack, pattern)
}
