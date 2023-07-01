package logql

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

// GroupingOp defines grouping operation.
type GroupingOp int

const (
	GroupingOpBy GroupingOp = iota + 1
	GroupingOpWithout
)
