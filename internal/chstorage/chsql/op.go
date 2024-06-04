package chsql

func unaryOp(op string, sub expr) expr {
	return expr{typ: exprUnaryOp, tok: op, args: []expr{sub}}
}

// Not returns new `NOT` operation.
func Not(sub expr) expr {
	return unaryOp("NOT", sub)
}

func binaryOp(left expr, op string, right expr) expr {
	return expr{typ: exprBinaryOp, tok: op, args: []expr{left, right}}
}

// Eq returns new `=` operation.
func Eq(left, right expr) expr {
	return binaryOp(left, "=", right)
}

// Gt returns new `>` operation.
func Gt(left, right expr) expr {
	return binaryOp(left, ">", right)
}

// Gte returns new `>=` operation.
func Gte(left, right expr) expr {
	return binaryOp(left, ">=", right)
}

// Lt returns new `<` operation.
func Lt(left, right expr) expr {
	return binaryOp(left, "<", right)
}

// Lte returns new `<=` operation.
func Lte(left, right expr) expr {
	return binaryOp(left, "<=", right)
}

// And returns new `AND` operation.
func And(left, right expr) expr {
	return binaryOp(left, "AND", right)
}

// Or returns new `OR` operation.
func Or(left, right expr) expr {
	return binaryOp(left, "OR", right)
}

// In returns new `IN` operation.
func In(left, right expr) expr {
	return binaryOp(left, "IN", right)
}
