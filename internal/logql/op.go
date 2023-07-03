package logql

import "fmt"

// BinOp defines binary operation.
type BinOp int

const (
	// Logical ops.
	OpAnd BinOp = iota + 1
	OpOr
	OpUnless
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
func (op BinOp) String() string {
	switch op {
	case OpAnd:
		return "and"
	case OpOr:
		return "or"
	case OpUnless:
		return "unless"
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

// IsLogic returns whether operation is logical.
func (op BinOp) IsLogic() bool {
	switch op {
	case OpAnd, OpOr, OpUnless:
		return true
	default:
		return false
	}
}

// RangeOp defines range aggregation operation.
type RangeOp int

const (
	RangeOpCount RangeOp = iota + 1
	RangeOpRate
	RangeOpRateCounter
	RangeOpBytes
	RangeOpBytesRate
	RangeOpAvg
	RangeOpSum
	RangeOpMin
	RangeOpMax
	RangeOpStdvar
	RangeOpStddev
	RangeOpQuantile
	RangeOpFirst
	RangeOpLast
	RangeOpAbsent
)

// VectorOp defines vector aggregation operation.
type VectorOp int

const (
	VectorOpSum VectorOp = iota + 1
	VectorOpAvg
	VectorOpCount
	VectorOpMax
	VectorOpMin
	VectorOpStddev
	VectorOpStdvar
	VectorOpBottomk
	VectorOpTopk
	VectorOpSort
	VectorOpSortDesc
)
