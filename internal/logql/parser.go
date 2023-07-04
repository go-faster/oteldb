package logql

import (
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql/lexer"
)

// Parse parses LogQL query from string.
func Parse(s string) (Expr, error) {
	tokens, err := lexer.Tokenize(s)
	if err != nil {
		return nil, errors.Wrap(err, "tokenize")
	}
	p := parser{
		tokens: tokens,
	}
	return p.parseExpr()
}

// ParseSelector parses label selector from string.
func ParseSelector(s string) (sel Selector, _ error) {
	tokens, err := lexer.Tokenize(s)
	if err != nil {
		return sel, errors.Wrap(err, "tokenize")
	}
	p := parser{
		tokens: tokens,
	}

	sel, err = p.parseSelector()
	if err != nil {
		return sel, err
	}
	if t := p.next(); t.Type != lexer.EOF {
		return sel, errors.Errorf("unexpected tail token %q", t.Type)
	}
	return sel, nil
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
	return errors.Errorf("unexpected token %q", t.Type)
}

func (p *parser) consumeText(tt lexer.TokenType) (string, error) {
	t := p.next()
	if t.Type != tt {
		return "", p.unexpectedToken(t)
	}
	return t.Text, nil
}

func (p *parser) parseIdent() (Label, error) {
	s, err := p.consumeText(lexer.Ident)
	return Label(s), err
}

func (p *parser) parseString() (string, error) {
	return p.consumeText(lexer.String)
}

func (p *parser) parseNumber() (float64, error) {
	text, err := p.consumeText(lexer.Number)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(text, 64)
}

func (p *parser) parseInt() (int, error) {
	text, err := p.consumeText(lexer.Number)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(text)
}

func (p *parser) parseDuration() (time.Duration, error) {
	text, err := p.consumeText(lexer.Duration)
	if err != nil {
		return 0, err
	}
	return lexer.ParseDuration(text)
}

func (p *parser) parseBytes() (uint64, error) {
	text, err := p.consumeText(lexer.Bytes)
	if err != nil {
		return 0, err
	}
	return humanize.ParseBytes(text)
}
