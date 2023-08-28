// Package lexer contains TraceQL lexer.
package lexer

import (
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/durationql"
)

type lexer struct {
	scanner scanner.Scanner
	tokens  []Token
	err     error
}

// TokenizeOptions is a Tokenize options structure.
type TokenizeOptions struct {
	// Filename sets filename for the scanner.
	Filename string
}

// Tokenize scans given string to LogQL tokens.
func Tokenize(s string, opts TokenizeOptions) ([]Token, error) {
	l := lexer{}
	l.scanner.Init(strings.NewReader(s))
	l.scanner.Filename = opts.Filename
	l.scanner.Error = func(s *scanner.Scanner, msg string) {
		l.err = errors.Errorf("scanner error: %s", msg)
	}

	for {
		r := l.scanner.Scan()
		switch r {
		case scanner.EOF:
			return l.tokens, l.err
		case '#':
			scanComment(&l.scanner)
			continue
		}

		tok, err := l.nextToken(r, l.scanner.TokenText())
		if err != nil {
			return l.tokens, err
		}
		l.tokens = append(l.tokens, tok)
	}
}

func (l *lexer) nextToken(r rune, text string) (tok Token, err error) {
	if r == '-' {
		if peekCh := l.scanner.Peek(); isDigit(peekCh) || peekCh == '.' {
			r = l.scanner.Scan()
			text = "-" + l.scanner.TokenText()
		}
	}

	tok.Text = text
	tok.Pos = l.scanner.Position
	switch r {
	case scanner.Float:
		switch r := l.scanner.Peek(); {
		case durationql.IsDurationRune(r):
			tok.Type = Duration
			tok.Text, err = durationql.ScanDuration(&l.scanner, text)
		default:
			tok.Type = Number
		}
		return tok, err
	case scanner.Int:
		switch r := l.scanner.Peek(); {
		case durationql.IsDurationRune(r):
			tok.Type = Duration
			tok.Text, err = durationql.ScanDuration(&l.scanner, text)
		default:
			tok.Type = Integer
		}
		return tok, err
	case scanner.String, scanner.RawString:
		tok.Type = String
		tok.Text, err = strconv.Unquote(text)
		return tok, err
	}
	peekCh := l.scanner.Peek()
	switch text {
	case "parent":
		if peekCh != '.' {
			// Just "parent".
			break
		}
		// "parent" followed by dot, it's attribute selector.
		fallthrough
	case ".", "resource", "span":
		// Attribute selector.
		var sb strings.Builder
		sb.WriteString(text)

		ch := peekCh
		for isAttributeRune(ch) {
			sb.WriteRune(l.scanner.Next())
			ch = l.scanner.Peek()
		}

		tok.Type = Ident
		tok.Text = sb.String()
		return tok, err
	}
	peeked := text + string(peekCh)

	tt, ok := tokens[peeked]
	if ok {
		tok.Type = tt
		tok.Text = peeked
		l.scanner.Next()
		return tok, nil
	}

	tt, ok = tokens[text]
	if ok {
		tok.Type = tt
		return tok, nil
	}

	tok.Type = Ident
	return tok, nil
}

func isAttributeRune(r rune) bool {
	if unicode.IsSpace(r) {
		return false
	}

	switch r {
	case scanner.EOF, '{', '}', '(', ')', '=', '~', '!', '<', '>', '&', '|', '^', ',':
		return false
	default:
		return true
	}
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func scanComment(s *scanner.Scanner) {
	for {
		ch := s.Next()
		if ch == scanner.EOF || ch == '\n' {
			break
		}
	}
}
