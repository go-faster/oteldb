package traceql

// Expr is a TraceQL expression.
type Expr interface {
	expr()
}

func (s *SpansetPipeline) expr() {}

// SpansetPipeline is a spanset pipeline.
type SpansetPipeline struct {
	Pipeline []PipelineStage
}
