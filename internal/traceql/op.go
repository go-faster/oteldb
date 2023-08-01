package traceql

import "fmt"

// BinaryOp defines binary operator.
type BinaryOp int

const (
	// Logical ops.
	OpAnd BinaryOp = iota + 1
	OpOr
	// Math ops.
	OpAdd
	OpSub
	OpMul
	OpDiv
	OpMod
	OpPow
	// Comparison ops.
	OpEq
	OpNotEq
	OpRe
	OpNotRe
	OpGt
	OpGte
	OpLt
	OpLte
)

// String implements fmt.Stringer.
func (op BinaryOp) String() string {
	switch op {
	case OpAnd:
		return "&&"
	case OpOr:
		return "||"
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpMod:
		return "%"
	case OpPow:
		return "^"
	case OpEq:
		return "="
	case OpNotEq:
		return "!="
	case OpRe:
		return "=~"
	case OpNotRe:
		return "!~"
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	default:
		return fmt.Sprintf("<unknown op %d>", op)
	}
}

// Precedence returns operator precedence.
func (op BinaryOp) Precedence() int {
	switch op {
	case OpOr, OpAnd:
		return 1
	case OpEq,
		OpNotEq,
		OpRe,
		OpNotRe,
		OpGt,
		OpGte,
		OpLt,
		OpLte:
		return 2
	case OpAdd, OpSub:
		return 3
	case OpMul, OpDiv, OpMod:
		return 4
	case OpPow:
		return 5
	default:
		return -1
	}
}

// CheckType checks if operator can be applied to given type.
func (op BinaryOp) CheckType(t StaticType) bool {
	if t == TypeAttribute {
		// Type is determined at execution time.
		return true
	}

	switch t {
	case TypeString:
		return op.IsOrdering() || op.IsRegex()
	case TypeInt, TypeNumber, TypeDuration:
		return op.IsArithmetic() || op.IsOrdering()
	case TypeBool:
		return op.IsEqual() || op.IsLogic()
	case TypeNil, TypeSpanStatus, TypeSpanKind:
		return op.IsEqual()
	default:
		return false
	}
}

// IsBoolean whether op have boolean result.
func (op BinaryOp) IsBoolean() bool {
	return op.IsLogic() || op.IsOrdering() || op.IsRegex()
}

// IsLogic whether op is logic operator.
func (op BinaryOp) IsLogic() bool {
	switch op {
	case OpAnd, OpOr:
		return true
	default:
		return false
	}
}

// IsOrdering whether op is ordering operator.
func (op BinaryOp) IsOrdering() bool {
	switch op {
	case OpEq,
		OpNotEq,
		OpGt,
		OpGte,
		OpLt,
		OpLte:
		return true
	default:
		return false
	}
}

// IsEqual whether op is equal operator.
func (op BinaryOp) IsEqual() bool {
	switch op {
	case OpEq, OpNotEq:
		return true
	default:
		return false
	}
}

// IsRegex whether op is regexp operator.
func (op BinaryOp) IsRegex() bool {
	switch op {
	case OpRe, OpNotRe:
		return true
	default:
		return false
	}
}

// IsArithmetic whether op is arithmetic operator.
func (op BinaryOp) IsArithmetic() bool {
	switch op {
	case OpAdd,
		OpSub,
		OpMul,
		OpDiv,
		OpMod,
		OpPow:
		return true
	default:
		return false
	}
}

// UnaryOp defines unary operator.
type UnaryOp int

const (
	OpNot UnaryOp = iota + 1
	OpNeg
)

// String implements fmt.Stringer.
func (op UnaryOp) String() string {
	switch op {
	case OpNot:
		return "!"
	case OpNeg:
		return "-"
	default:
		return fmt.Sprintf("<unknown op %d>", op)
	}
}

// CheckType checks if operator can be applied to given type.
func (op UnaryOp) CheckType(t StaticType) bool {
	if t == TypeAttribute {
		// Type is determined at execution time.
		return true
	}

	switch op {
	case OpNeg:
		return t.IsNumeric()
	case OpNot:
		return t == TypeBool
	default:
		return false
	}
}

// SpansetOp defines spanset operator.
type SpansetOp int

const (
	SpansetOpAnd SpansetOp = iota + 1
	SpansetOpChild
	SpansetOpDescendant
	SpansetOpUnion
	SpansetOpSibling
)

// String implements fmt.Stringer.
func (op SpansetOp) String() string {
	switch op {
	case SpansetOpAnd:
		return "&&"
	case SpansetOpChild:
		return "<"
	case SpansetOpDescendant:
		return ">>"
	case SpansetOpUnion:
		return "||"
	case SpansetOpSibling:
		return "~"
	default:
		return fmt.Sprintf("<unknown op %d>", op)
	}
}

// Precedence returns operator precedence.
func (op SpansetOp) Precedence() int {
	switch op {
	case SpansetOpAnd, SpansetOpUnion:
		return 1
	case SpansetOpChild,
		SpansetOpDescendant,
		SpansetOpSibling:
		return 2
	default:
		return -1
	}
}

// AggregateOp defines aggregation operator.
type AggregateOp int

const (
	AggregateOpCount AggregateOp = iota + 1
	AggregateOpMax
	AggregateOpMin
	AggregateOpAvg
	AggregateOpSum
)

// String implements fmt.Stringer.
func (op AggregateOp) String() string {
	switch op {
	case AggregateOpCount:
		return "count"
	case AggregateOpMax:
		return "max"
	case AggregateOpMin:
		return "min"
	case AggregateOpAvg:
		return "avg"
	case AggregateOpSum:
		return "sum"
	default:
		return fmt.Sprintf("<unknown op %d>", op)
	}
}
