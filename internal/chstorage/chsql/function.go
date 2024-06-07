package chsql

// Function reutrns function call expression.
func Function(name string, args ...Expr) Expr {
	return Expr{typ: exprFunction, tok: name, args: args}
}

// ToString returns `toString(<arg>)` function call expression.
func ToString(arg Expr) Expr {
	return Function("toString", arg)
}

// ToFloat64 returns `toFloat64(<arg>)` function call expression.
func ToFloat64(arg Expr) Expr {
	return Function("toFloat64", arg)
}

// ToUnixTimestamp64Nano returns `toUnixTimestamp64Nano(<arg>)` function call expression.
func ToUnixTimestamp64Nano(arg Expr) Expr {
	return Function("toUnixTimestamp64Nano", arg)
}

// Coalesce returns `coalesce(<args>...)` function call expression.
func Coalesce(args ...Expr) Expr {
	return Function("coalesce", args...)
}

// Has returns `has(<arr>, <elem>)` function call expression.
func Has(arr, elem Expr) Expr {
	return Function("has", arr, elem)
}

// Map returns `map(<args>...)` function call expression.
func Map(args ...Expr) Expr {
	return Function("map", args...)
}

// MapConcat returns `mapConcat(<args>...)` function call expression.
func MapConcat(args ...Expr) Expr {
	return Function("mapConcat", args...)
}

// Array returns `array(<args>...)` function call expression.
func Array(args ...Expr) Expr {
	return Function("array", args...)
}

// ArrayConcat returns `arrayConcat(<args>...)` function call expression.
func ArrayConcat(args ...Expr) Expr {
	return Function("arrayConcat", args...)
}

// ArrayJoin returns `arrayJoin(<args>...)` function call expression.
func ArrayJoin(args ...Expr) Expr {
	return Function("arrayJoin", args...)
}

// Hex returns `hex(<arg>)` function call expression.
func Hex(arg Expr) Expr {
	return Function("hex", arg)
}

// Unhex returns `unhex(<arg>)` function call expression.
func Unhex(arg Expr) Expr {
	return Function("unhex", arg)
}

// Length returns `length(<arg>)` function call expression.
func Length(arg Expr) Expr {
	return Function("length", arg)
}

// PositionUTF8 returns `positionUTF8(<haystack>, <needle>)` function call expression.
func PositionUTF8(haystack, needle Expr) Expr {
	return Function("positionUTF8", haystack, needle)
}

// Match returns `match(<haystack>, <pattern>)` function call expression.
func Match(haystack, pattern Expr) Expr {
	return Function("match", haystack, pattern)
}

// JSONExtract returns `JSONExtract(<from>, <typ>)` function call expression.
func JSONExtract(from Expr, typ string) Expr {
	return Function("JSONExtract", from, String(typ))
}

// JSONExtractField returns `JSONExtract(<from>, <field>, <typ>)` function call expression.
func JSONExtractField(from Expr, field, typ string) Expr {
	return Function("JSONExtract", from, String(field), String(typ))
}

// JSONExtractKeys returns `JSONExtractKeys(<from>)` function call expression.
func JSONExtractKeys(from Expr) Expr {
	return Function("JSONExtractKeys", from)
}

// JSONExtractString returns `JSONExtractString(<from>, <field>)` function call expression.
func JSONExtractString(from Expr, field string) Expr {
	return Function("JSONExtractString", from, String(field))
}
