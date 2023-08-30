// Package lexer contains TraceQL lexer.
package lexer

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/go-faster/oteldb/internal/lexerql"
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
		l.setError(msg, l.scanner.Position)
	}

	for {
		r := l.scanner.Scan()
		switch r {
		case scanner.EOF:
			return l.tokens, l.err
		case '#':
			lexerql.ScanComment(&l.scanner)
			continue
		}

		tok, ok := l.nextToken(r, l.scanner.TokenText())
		if !ok {
			return l.tokens, l.err
		}
		l.tokens = append(l.tokens, tok)
	}
}

func (l *lexer) setError(msg string, pos scanner.Position) {
	l.err = &Error{
		Msg: msg,
		Pos: pos,
	}
}

func (l *lexer) nextToken(r rune, text string) (tok Token, _ bool) {
	tok.Pos = l.scanner.Position
	if r == '-' {
		if peekCh := l.scanner.Peek(); lexerql.IsDigit(peekCh) || peekCh == '.' {
			r = l.scanner.Scan()
			text = "-" + l.scanner.TokenText()
		}
	}
	tok.Text = text

	switch r {
	case scanner.Float:
		switch r := l.scanner.Peek(); {
		case lexerql.IsDurationRune(r):
			duration, err := lexerql.ScanDuration(&l.scanner, text)
			if err != nil {
				l.setError(err.Error(), tok.Pos)
				return tok, false
			}
			tok.Type = Duration
			tok.Text = duration
		default:
			tok.Type = Number
		}
		return tok, true
	case scanner.Int:
		switch r := l.scanner.Peek(); {
		case lexerql.IsDurationRune(r):
			duration, err := lexerql.ScanDuration(&l.scanner, text)
			if err != nil {
				l.setError(err.Error(), tok.Pos)
				return tok, false
			}
			tok.Type = Duration
			tok.Text = duration
		default:
			tok.Type = Integer
		}
		return tok, true
	case scanner.String, scanner.RawString:
		unquoted, err := strconv.Unquote(text)
		if err != nil {
			l.setError(fmt.Sprintf("unquote string: %s", err), tok.Pos)
			return tok, false
		}
		tok.Type = String
		tok.Text = unquoted
		return tok, true
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
		return tok, true
	}
	peeked := text + string(peekCh)

	tt, ok := tokens[peeked]
	if ok {
		tok.Type = tt
		tok.Text = peeked
		l.scanner.Next()
		return tok, true
	}

	tt, ok = tokens[text]
	if ok {
		tok.Type = tt
		return tok, true
	}

	tok.Type = Ident
	return tok, true
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
