package traceql

// PipelineStage is a pipeline stage.
type PipelineStage interface {
	pipelineStage()
}

func (*BinarySpansetExpr) pipelineStage() {}
func (*SpansetFilter) pipelineStage()     {}
func (*ScalarFilter) pipelineStage()      {}
func (*GroupOperation) pipelineStage()    {}
func (*CoalesceOperation) pipelineStage() {}
func (*SelectOperation) pipelineStage()   {}

// SpansetExpr is a spanset expression.
type SpansetExpr interface {
	spansetExpr()
	PipelineStage
}

func (*BinarySpansetExpr) spansetExpr() {}
func (*SpansetFilter) spansetExpr()     {}

// BinarySpansetExpr is a binary operation between two spanset expressions.
type BinarySpansetExpr struct {
	Left  SpansetExpr
	Op    SpansetOp
	Right SpansetExpr
}

// SpansetFilter is a spanset filter.
type SpansetFilter struct {
	Expr FieldExpr // if filter is empty, expr is True
}

// ScalarFilter is a scalar filter.
type ScalarFilter struct {
	Left  ScalarExpr
	Op    BinaryOp
	Right ScalarExpr
}

// GroupOperation is a `by()` operation.
type GroupOperation struct {
	By FieldExpr
}

// CoalesceOperation is a `colaesce()` operation.
type CoalesceOperation struct{}

// SelectOperation is a `select()` operation.
type SelectOperation struct {
	Args []FieldExpr
}
