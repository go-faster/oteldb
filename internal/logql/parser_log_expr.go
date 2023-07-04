package logql

import (
	"regexp"

	"github.com/go-faster/errors"
	"github.com/go-faster/oteldb/internal/logql/lexer"
)

func (p *parser) parseLogExpr() (e *LogExpr, err error) {
	e = new(LogExpr)

	e.Sel, err = p.parseSelector()
	if err != nil {
		return e, err
	}

	e.Pipeline, err = p.parsePipeline(false)
	if err != nil {
		return e, err
	}

	return e, nil
}

func (p *parser) parseSelector() (s Selector, err error) {
	switch t := p.next(); t.Type {
	case lexer.OpenParen:
		s, err = p.parseSelector()
		if err != nil {
			return s, err
		}
		err = p.consume(lexer.CloseParen)
		return s, err
	case lexer.OpenBrace:
	default:
		return s, p.unexpectedToken(t)
	}

	// Empty selector.
	if t := p.peek(); t.Type == lexer.CloseBrace {
		p.next()
		return s, nil
	}

	for {
		m, err := p.parseLabelMatcher()
		if err != nil {
			return s, err
		}
		s.Matchers = append(s.Matchers, m)

		switch t := p.next(); t.Type {
		case lexer.CloseBrace:
			return s, nil
		case lexer.Comma:
		default:
			return s, p.unexpectedToken(t)
		}
	}
}

func (p *parser) parseLabelMatcher() (m LabelMatcher, err error) {
	m.Label, err = p.parseIdent()
	if err != nil {
		return m, err
	}

	switch t := p.next(); t.Type {
	case lexer.Eq:
		m.Op = OpEq
	case lexer.NotEq:
		m.Op = OpNotEq
	case lexer.Re:
		m.Op = OpRe
	case lexer.NotRe:
		m.Op = OpNotRe
	default:
		return m, p.unexpectedToken(t)
	}

	m.Value, err = p.parseString()
	if err != nil {
		return m, err
	}
	switch m.Op {
	case OpRe, OpNotRe:
		m.Re, err = regexp.Compile("^(?:" + m.Value + ")$")
		if err != nil {
			return m, errors.Wrapf(err, "invalid regex in label matcher %q", m.Value)
		}
	}
	return m, nil
}
