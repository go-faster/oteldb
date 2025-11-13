package chsql

// SimpleJSONHas returns `simpleJSONHas(<json>, <field>)` function call expression.
func SimpleJSONHas(json Expr, field string) Expr {
	return Function("simpleJSONHas", json, String(field))
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
