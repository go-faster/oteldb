package traceql

// PipelineStage is a pipeline stage.
type PipelineStage interface {
	pipelineStage()
}

func (*BinarySpansetExpr) pipelineStage() {}
func (*SpansetFilter) pipelineStage()     {}
func (*ScalarFilter) pipelineStage()      {}

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
