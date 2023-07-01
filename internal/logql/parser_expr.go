package logql

import "github.com/go-faster/oteldb/internal/logql/lexer"

func (p *parser) parseExpr() (expr Expr, err error) {
	switch t := p.peek(); t.Type {
	case lexer.OpenBrace:
		expr, err = p.parseLogExpr()
		if err != nil {
			return nil, err
		}
	case lexer.OpenParen:
		p.next()

		expr, err = p.parseExpr()
		if err != nil {
			return nil, err
		}

		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}

		expr = &ParenExpr{X: expr}
	default:
		expr, err = p.parseMetricExpr()
		if err != nil {
			return nil, err
		}
	}

	var binOp BinOp
	switch t := p.peek(); t.Type {
	case lexer.Or:
		binOp = OpOr
	case lexer.And:
		binOp = OpAnd
	case lexer.Unless:
		binOp = OpUnless
	case lexer.Add:
		binOp = OpAdd
	case lexer.Sub:
		binOp = OpSub
	case lexer.Mul:
		binOp = OpMul
	case lexer.Div:
		binOp = OpDiv
	case lexer.Mod:
		binOp = OpMod
	case lexer.Pow:
		binOp = OpPow
	case lexer.CmpEq:
		binOp = OpEq
	case lexer.NotEq:
		binOp = OpNotEq
	case lexer.Gt:
		binOp = OpGt
	case lexer.Gte:
		binOp = OpGte
	case lexer.Lt:
		binOp = OpLt
	case lexer.Lte:
		binOp = OpLte
	default:
		return expr, err
	}
	p.next()

	modifier, err := p.parseBinOpModifier()
	if err != nil {
		return nil, err
	}

	right, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	return &BinOpExpr{Left: expr, Op: binOp, Modifier: modifier, Right: right}, nil
}

func (p *parser) parseBinOpModifier() (bm BinOpModifier, err error) {
	if t := p.peek(); t.Type == lexer.Bool {
		p.next()
		bm.ReturnBool = true
	}

	switch t := p.peek(); t.Type {
	case lexer.On:
		bm.Op = "on"
	case lexer.Ignoring:
		bm.Op = "ignoring"
	default:
		return bm, nil
	}
	p.next()

	bm.OpLabels, err = p.parseLabels()
	if err != nil {
		return bm, err
	}

	switch t := p.peek(); t.Type {
	case lexer.GroupLeft:
		bm.Group = "left"
	case lexer.GroupRight:
		bm.Group = "right"
	default:
		return bm, nil
	}
	p.next()

	// Try to read '('.
	if t := p.peek(); t.Type != lexer.OpenParen {
		return bm, nil
	}
	p.next()

	switch t := p.peek(); t.Type {
	case lexer.CloseParen:
		// Empty include list.
		// Consume ')'
		p.next()
		return bm, nil
	case lexer.Ident:
		// Non-empty include list.
		// Unread leading paren.
		p.unread()

		bm.Include, err = p.parseLabels()
		if err != nil {
			return bm, err
		}
		return bm, nil
	default:
		// If after '(' goes something else, it's the next expression.
		p.unread()
		return bm, nil
	}
}
