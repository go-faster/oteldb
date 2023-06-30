package logql

// CmpOp defines comparison operation.
type CmpOp int

const (
	CmpEq CmpOp = iota + 1
	CmpNotEq
	CmpGt
	CmpGte
	CmpLt
	CmpLte
	CmpRe
	CmpNotRe
)

// LogOp defines logic operation.
type LogOp int

const (
	LogOpAnd LogOp = iota + 1
	LogOpOr
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
