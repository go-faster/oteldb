package traceql

import (
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/traceql/lexer"
)

func (p *parser) parsePipeline() (stages []PipelineStage, _ error) {
	for {
		n := 0
		for {
			if t := p.peek(); t.Type != lexer.OpenParen {
				switch t.Type {
				case lexer.OpenBrace,
					lexer.Integer,
					lexer.Number,
					lexer.Duration,
					lexer.Count,
					lexer.Max,
					lexer.Min,
					lexer.Avg,
					lexer.Sum:
				default:
					if n > 0 {
						p.unread()
						return stages, p.unexpectedToken(p.next())
					}
				}
				break
			}
			p.next()
			n++
		}
		unreadParens := func() {
			for i := 0; i < n; i++ {
				p.unread()
			}
		}
		switch t := p.peek(); t.Type {
		case lexer.OpenBrace:
			unreadParens()
			expr, err := p.parseSpansetExpr()
			if err != nil {
				return stages, err
			}
			stages = append(stages, expr)
		case lexer.By:
			p.next()

			op, err := p.parseGroupOperation()
			if err != nil {
				return stages, err
			}
			stages = append(stages, op)
		case lexer.Coalesce:
			if len(stages) < 1 {
				return stages, errors.Errorf("coalesce cannot be first operation: at %s", t.Pos)
			}
			p.next()

			op, err := p.parseCoalesceOperation()
			if err != nil {
				return stages, err
			}
			stages = append(stages, op)
		case lexer.Select:
			p.next()

			op, err := p.parseSelectOperation()
			if err != nil {
				return stages, err
			}
			stages = append(stages, op)
		case lexer.Integer,
			lexer.Number,
			lexer.Duration,
			lexer.Count,
			lexer.Max,
			lexer.Min,
			lexer.Avg,
			lexer.Sum:
			unreadParens()
			expr, err := p.parseScalarFilter()
			if err != nil {
				return stages, err
			}
			stages = append(stages, expr)
		default:
			return stages, p.unexpectedToken(t)
		}

		if t := p.peek(); t.Type != lexer.Pipe {
			return stages, nil
		}
		// Consume "|".
		p.next()
	}
}

func (p *parser) parseSpansetExpr() (SpansetExpr, error) {
	expr, err := p.parseSpansetExpr1()
	if err != nil {
		return nil, err
	}
	return p.parseBinarySpansetExpr(expr, 0)
}

func (p *parser) parseSpansetExpr1() (SpansetExpr, error) {
	switch t := p.next(); t.Type {
	case lexer.OpenParen:
		expr, err := p.parseSpansetExpr()
		if err != nil {
			return nil, err
		}

		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}
		return &ParenSpansetExpr{Expr: expr}, nil
	case lexer.OpenBrace:
		var filter SpansetFilter
		if t2 := p.peek(); t2.Type != lexer.CloseBrace {
			fieldExpr, err := p.parseFieldExpr()
			if err != nil {
				return nil, err
			}
			filter.Expr = fieldExpr
		} else {
			s := &Static{}
			s.SetBool(true)
			filter.Expr = s
		}

		if err := p.consume(lexer.CloseBrace); err != nil {
			return nil, err
		}

		return &filter, nil
	default:
		return nil, p.unexpectedToken(t)
	}
}

func (p *parser) parseBinarySpansetExpr(left SpansetExpr, minPrecedence int) (SpansetExpr, error) {
	for {
		op, ok := p.peekSpansetOp()
		if !ok || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op.
		p.next()

		right, err := p.parseSpansetExpr1()
		if err != nil {
			return nil, err
		}

		for {
			rightOp, ok := p.peekSpansetOp()
			if !ok || rightOp.Precedence() < op.Precedence() {
				break
			}

			right, err = p.parseBinarySpansetExpr(right, minPrecedence+1)
			if err != nil {
				return nil, err
			}
		}

		left = &BinarySpansetExpr{Left: left, Op: op, Right: right}
	}
}

func (p *parser) peekSpansetOp() (op SpansetOp, _ bool) {
	switch t := p.peek(); t.Type {
	case lexer.And:
		return SpansetOpAnd, true
	case lexer.Gt:
		return SpansetOpChild, true
	case lexer.Desc:
		return SpansetOpDescendant, true
	case lexer.Or:
		return SpansetOpUnion, true
	case lexer.Tilde:
		return SpansetOpSibling, true
	default:
		return op, false
	}
}

func (p *parser) parseScalarFilter() (*ScalarFilter, error) {
	left, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}

	var op BinaryOp
	switch t := p.next(); t.Type {
	case lexer.Eq:
		op = OpEq
	case lexer.NotEq:
		op = OpNotEq
	case lexer.Gt:
		op = OpGt
	case lexer.Gte:
		op = OpGte
	case lexer.Lt:
		op = OpLt
	case lexer.Lte:
		op = OpLte
	default:
		return nil, p.unexpectedToken(t)
	}

	right, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}

	return &ScalarFilter{Left: left, Op: op, Right: right}, nil
}

func (p *parser) parseGroupOperation() (*GroupOperation, error) {
	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	field, err := p.parseFieldExpr()
	if err != nil {
		return nil, err
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	return &GroupOperation{By: field}, nil
}

func (p *parser) parseCoalesceOperation() (*CoalesceOperation, error) {
	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	return &CoalesceOperation{}, nil
}

func (p *parser) parseSelectOperation() (s *SelectOperation, _ error) {
	s = new(SelectOperation)

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	for {
		field, err := p.parseFieldExpr()
		if err != nil {
			return nil, err
		}
		s.Args = append(s.Args, field)

		if t := p.peek(); t.Type != lexer.Comma {
			break
		}
		// Consume comma.
		p.next()
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	return s, nil
}
