package traceql

// ScalarExpr is a scalar expression.
type ScalarExpr interface {
	scalarExpr()
}

func (*BinaryScalarExpr) scalarExpr()    {}
func (*Static) scalarExpr()              {}
func (*AggregateScalarExpr) scalarExpr() {}

// BinaryScalarExpr is a binary operation between two scalar expressions.
type BinaryScalarExpr struct {
	Left  ScalarExpr
	Op    BinaryOp
	Right ScalarExpr
}

// AggregateScalarExpr is an aggregate function.
type AggregateScalarExpr struct {
	Op    AggregateOp
	Field FieldExpr // nilable
}
