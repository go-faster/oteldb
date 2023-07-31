package traceql

// Expr is a TraceQL expression.
type Expr interface {
	expr()
}

func (s *BinaryExpr) expr()      {}
func (s *SpansetPipeline) expr() {}

// BinaryExpr is a binary expression.
type BinaryExpr struct {
	Left  Expr
	Op    SpansetOp
	Right Expr
}

// SpansetPipeline is a spanset pipeline.
type SpansetPipeline struct {
	Pipeline []PipelineStage
}
