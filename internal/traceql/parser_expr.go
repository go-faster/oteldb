package traceql

func (p *parser) parseExpr() (Expr, error) {
	expr, err := p.parseExpr1()
	if err != nil {
		return nil, err
	}
	return p.parseBinaryExpr(expr, 0)
}

func (p *parser) parseExpr1() (Expr, error) {
	pipeline, err := p.parsePipeline()
	if err != nil {
		return nil, err
	}
	return &SpansetPipeline{Pipeline: pipeline}, nil
}

func (p *parser) parseBinaryExpr(left Expr, minPrecedence int) (Expr, error) {
	for {
		op, ok := p.peekSpansetOp()
		if !ok || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op.
		p.next()

		right, err := p.parseExpr1()
		if err != nil {
			return nil, err
		}

		for {
			rightOp, ok := p.peekSpansetOp()
			if !ok || rightOp.Precedence() < op.Precedence() {
				break
			}

			right, err = p.parseBinaryExpr(right, minPrecedence+1)
			if err != nil {
				return nil, err
			}
		}

		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}
}
