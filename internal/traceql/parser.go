package traceql

import (
	"strconv"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/durationql"
	"github.com/go-faster/oteldb/internal/traceql/lexer"
)

// Parse parses TraceQL query from string.
func Parse(s string) (Expr, error) {
	tokens, err := lexer.Tokenize(s, lexer.TokenizeOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "tokenize")
	}
	p := parser{
		tokens: tokens,
	}
	return p.parseExpr()
}

type parser struct {
	tokens []lexer.Token
	pos    int
}

func (p *parser) consume(tt lexer.TokenType) error {
	if t := p.next(); t.Type != tt {
		return p.unexpectedToken(t)
	}
	return nil
}

func (p *parser) next() lexer.Token {
	t := p.peek()
	if t.Type != lexer.EOF {
		p.pos++
	}
	return t
}

func (p *parser) peek() lexer.Token {
	if len(p.tokens) <= p.pos {
		return lexer.Token{Type: lexer.EOF}
	}
	return p.tokens[p.pos]
}

func (p *parser) unread() {
	if p.pos > 0 {
		p.pos--
	}
}

func (p *parser) unexpectedToken(t lexer.Token) error {
	if t.Type == lexer.EOF {
		return errors.New("unexpected EOF")
	}
	return errors.Errorf("unexpected token %q at %s", t.Type, t.Pos)
}

func (p *parser) consumeText(tt lexer.TokenType) (string, error) {
	t := p.next()
	if t.Type != tt {
		return "", errors.Wrapf(p.unexpectedToken(t), "expected %q", tt)
	}
	return t.Text, nil
}

func (p *parser) parseString() (string, error) {
	s, err := p.consumeText(lexer.String)
	return s, err
}

func (p *parser) parseInteger() (int64, error) {
	text, err := p.consumeText(lexer.Integer)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(text, 0, 64)
}

func (p *parser) parseNumber() (float64, error) {
	text, err := p.consumeText(lexer.Number)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(text, 64)
}

func (p *parser) parseDuration() (time.Duration, error) {
	text, err := p.consumeText(lexer.Duration)
	if err != nil {
		return 0, err
	}
	return durationql.ParseDuration(text)
}
