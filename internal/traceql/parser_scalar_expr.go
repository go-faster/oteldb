package traceql

import "github.com/go-faster/oteldb/internal/traceql/lexer"

func (p *parser) parseScalarExpr() (ScalarExpr, error) {
	expr, err := p.parseScalarExpr1()
	if err != nil {
		return nil, err
	}
	return p.parseBinaryScalarExpr(expr, 0)
}

func (p *parser) parseScalarExpr1() (ScalarExpr, error) {
	switch t := p.peek(); t.Type {
	case lexer.OpenParen:
		p.next()

		expr, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}

		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}
		return &ParenScalarExpr{Expr: expr}, nil
	case lexer.Integer,
		lexer.Number,
		lexer.Duration:
		return p.parseStatic()
	case lexer.Count,
		lexer.Max,
		lexer.Min,
		lexer.Avg,
		lexer.Sum:
		return p.parseAggregateScalarExpr()
	default:
		return nil, p.unexpectedToken(t)
	}
}

func (p *parser) parseAggregateScalarExpr() (expr *AggregateScalarExpr, _ error) {
	expr = new(AggregateScalarExpr)
	switch t := p.next(); t.Type {
	case lexer.Count:
		expr.Op = AggregateOpCount
	case lexer.Max:
		expr.Op = AggregateOpMax
	case lexer.Min:
		expr.Op = AggregateOpMin
	case lexer.Avg:
		expr.Op = AggregateOpAvg
	case lexer.Sum:
		expr.Op = AggregateOpSum
	default:
		return nil, p.unexpectedToken(t)
	}

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	if expr.Op != AggregateOpCount {
		field, err := p.parseFieldExpr()
		if err != nil {
			return nil, err
		}
		expr.Field = field
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	return expr, nil
}

func (p *parser) parseBinaryScalarExpr(left ScalarExpr, minPrecedence int) (ScalarExpr, error) {
	for {
		op, ok := p.peekBinaryOp()
		if !ok || !op.IsArithmetic() || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op.
		p.next()

		right, err := p.parseScalarExpr1()
		if err != nil {
			return nil, err
		}

		for {
			rightOp, ok := p.peekBinaryOp()
			if !ok || !op.IsArithmetic() || rightOp.Precedence() < op.Precedence() {
				break
			}

			right, err = p.parseBinaryScalarExpr(right, minPrecedence+1)
			if err != nil {
				return nil, err
			}
		}

		left = &BinaryScalarExpr{Left: left, Op: op, Right: right}
	}
}
