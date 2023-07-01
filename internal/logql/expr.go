package logql

// Expr is a root LogQL expression.
type Expr interface {
	expr()
}

func (*ParenExpr) expr() {}
func (*BinOpExpr) expr() {}

// ParenExpr is parenthesized Expr.
type ParenExpr struct {
	X Expr
}

// BinOpExpr defines a binary operation between two Expr.
type BinOpExpr struct {
	Left     Expr
	Op       BinOp
	Modifier BinOpModifier
	Right    Expr
}

// BinOpModifier defines BinOpExpr modifier.
//
// FIXME(tdakkota): this feature is not well documented.
type BinOpModifier struct {
	Op         string // on, ignoring
	OpLabels   []Label
	Group      string // "", "left", "right"
	Include    []Label
	ReturnBool bool
}
