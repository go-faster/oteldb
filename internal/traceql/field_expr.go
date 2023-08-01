package traceql

// FieldExpr is a field expression.
type FieldExpr interface {
	fieldExpr()
	ValueType() StaticType
}

func (*BinaryFieldExpr) fieldExpr() {}
func (*UnaryFieldExpr) fieldExpr()  {}
func (*Static) fieldExpr()          {}
func (*Attribute) fieldExpr()       {}

// BinaryFieldExpr is a binary operation between two field expressions.
type BinaryFieldExpr struct {
	Left  FieldExpr
	Op    BinaryOp
	Right FieldExpr
}

// ValueType returns value type of expression.
func (s *BinaryFieldExpr) ValueType() StaticType {
	if s.Op.IsBoolean() {
		return TypeBool
	}

	t := s.Left.ValueType()
	if t != TypeAttribute {
		return t
	}
	return s.Right.ValueType()
}

// UnaryFieldExpr is a unary field expression operation.
type UnaryFieldExpr struct {
	Expr FieldExpr
	Op   UnaryOp
}

// ValueType returns value type of expression.
func (s *UnaryFieldExpr) ValueType() StaticType {
	return s.Expr.ValueType()
}
