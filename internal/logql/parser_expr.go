package logql

import "github.com/go-faster/oteldb/internal/logql/lexer"

// Expr is a root LogQL expression.
type Expr interface {
	expr()
}

// Label is a LogQL identifier.
type Label string

func (p *parser) parseExpr() (Expr, error) {
	switch t := p.peek(); t.Type {
	case lexer.OpenBrace:
		return p.parseLogExpr()
	case lexer.OpenParen:
		p.next()

		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		err = p.consume(lexer.CloseParen)
		return expr, err
	default:
		return p.parseMetricExpr()
	}
}
