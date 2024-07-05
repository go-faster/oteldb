package logql

// Expr is a root LogQL expression.
type Expr interface {
	expr()
}

// ParenExpr is parenthesized Expr.
type ParenExpr struct {
	X Expr
}

func (*ParenExpr) expr()       {}
func (*ParenExpr) metricExpr() {}

// UnparenExpr recursively extracts expression from parentheses.
func UnparenExpr(e Expr) Expr {
	p, ok := e.(*ParenExpr)
	if !ok {
		return e
	}
	return UnparenExpr(p.X)
}

// ExplainExpr is a wrapper around Expr to explain.
type ExplainExpr struct {
	X Expr
}

func (*ExplainExpr) expr()       {}
func (*ExplainExpr) metricExpr() {}
