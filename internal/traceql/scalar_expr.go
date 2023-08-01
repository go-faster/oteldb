package traceql

// ScalarExpr is a scalar expression.
type ScalarExpr interface {
	scalarExpr()
	ValueType() StaticType
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

// ValueType returns value type of expression.
func (s *BinaryScalarExpr) ValueType() StaticType {
	if s.Op.IsBoolean() {
		return TypeBool
	}

	t := s.Left.ValueType()
	if t != TypeAttribute {
		return t
	}
	return s.Right.ValueType()
}

// AggregateScalarExpr is an aggregate function.
type AggregateScalarExpr struct {
	Op    AggregateOp
	Field FieldExpr // nilable
}

// ValueType returns value type of expression.
func (s *AggregateScalarExpr) ValueType() StaticType {
	if s.Op == AggregateOpCount {
		return TypeInt
	}
	return s.Field.ValueType()
}
