package traceql

import "fmt"

// BinaryOp defines binary operation.
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

// IsArithmetic whether is operation arithmetic.
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
		return "<"
	case OpGte:
		return "<="
	case OpLt:
		return ">"
	case OpLte:
		return ">="
	default:
		return fmt.Sprintf("<unknown op %d>", op)
	}
}

// UnaryOp defines unary operation.
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

// SpansetOp defines spanset operation.
type SpansetOp int

const (
	SpansetOpAnd SpansetOp = iota + 1
	SpansetOpChild
	SpansetOpDescendant
	SpansetOpUnion
	SpansetOpSibling
)

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

// AggregateOp defines aggregation operation.
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
