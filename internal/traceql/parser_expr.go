package traceql

func (p *parser) parseExpr() (Expr, error) {
	pipeline, err := p.parsePipeline()
	if err != nil {
		return nil, err
	}
	return &SpansetPipeline{Pipeline: pipeline}, nil
}
