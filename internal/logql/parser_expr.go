package logql

import (
	"github.com/go-faster/oteldb/internal/logql/lexer"
)

func (p *parser) parseExpr() (expr Expr, err error) {
	switch t := p.peek(); t.Type {
	case lexer.OpenBrace:
		return p.parseLogExpr()
	default:
		return p.parseMetricExpr()
	}
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
