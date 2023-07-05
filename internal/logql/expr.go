package logql

// Expr is a root LogQL expression.
type Expr interface {
	expr()
}

func (*ParenExpr) expr() {}

func (*ParenExpr) metricExpr() {}

// ParenExpr is parenthesized Expr.
type ParenExpr struct {
	X Expr
}

// UnparenExpr recursively extracts expression from parentheses.
func UnparenExpr(e Expr) Expr {
	p, ok := e.(*ParenExpr)
	if !ok {
		return e
	}
	return UnparenExpr(p.X)
}
