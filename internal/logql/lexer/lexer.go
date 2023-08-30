// Package lexer contains LogQL lexer.
package lexer

import (
	"fmt"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/prometheus/util/strutil"

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
	// AllowDots allows dots in identifiers.
	AllowDots bool
}

// Tokenize scans given string to LogQL tokens.
func Tokenize(s string, opts TokenizeOptions) ([]Token, error) {
	l := lexer{}
	l.scanner.Init(strings.NewReader(s))
	l.scanner.Filename = opts.Filename
	if opts.AllowDots {
		l.scanner.IsIdentRune = func(ch rune, i int) bool {
			return ch == '_' || unicode.IsLetter(ch) ||
				(unicode.IsDigit(ch) && i > 0) ||
				(ch == '.' && i > 0) // allow dot if it is not the first character of token
		}
	}
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
	tok.Text = text
	if r == '-' && l.scanner.Peek() == '-' {
		tok.Type = ParserFlag
		tok.Text = scanFlag(&l.scanner, text)
		return tok, true
	}
	switch r {
	case scanner.Int, scanner.Float:
		switch r := l.scanner.Peek(); {
		case lexerql.IsDurationRune(r):
			duration, err := lexerql.ScanDuration(&l.scanner, text)
			if err != nil {
				l.setError(err.Error(), tok.Pos)
				return tok, false
			}
			tok.Type = Duration
			tok.Text = duration
		case isBytesRune(r):
			bytes, err := scanBytes(&l.scanner, text)
			if err != nil {
				l.setError(err.Error(), tok.Pos)
				return tok, false
			}
			tok.Type = Bytes
			tok.Text = bytes
		default:
			tok.Type = Number
		}
		return tok, true
	case scanner.String, scanner.RawString:
		// FIXME(tdakkota): requires a huge dependency
		unquoted, err := strutil.Unquote(text)
		if err != nil {
			l.setError(fmt.Sprintf("unquote string: %s", err), tok.Pos)
			return tok, false
		}
		tok.Type = String
		tok.Text = unquoted
		return tok, true
	}
	peekCh := l.scanner.Peek()
	peeked := text + string(peekCh)

	tt, ok := tokens[peeked]
	if ok {
		l.scanner.Next()
		tok.Type = tt
		tok.Text = peeked
		return tok, true
	}

	tt, ok = tokens[text]
	if ok {
		tok.Type = tt
		// FIXME(tdakkota): does it work in all cases?
		if tt.IsFunction() {
			scanSpace(&l.scanner)
			switch l.scanner.Peek() {
			case '(', 'b', 'w': // "(", "by", "without"
			default:
				// Identifier can also have name 'duration', 'ip', etc.
				tok.Type = Ident
			}
		}
		return tok, true
	}

	tok.Type = Ident
	return tok, true
}

func scanSpace(s *scanner.Scanner) {
	for {
		if ch := s.Peek(); !unicode.IsSpace(ch) {
			return
		}
		s.Next()
	}
}

func scanBytes(s *scanner.Scanner, number string) (string, error) {
	var sb strings.Builder
	sb.WriteString(number)

	for {
		ch := s.Peek()
		if !lexerql.IsDigit(ch) && !isBytesRune(ch) && ch != '.' {
			break
		}
		sb.WriteRune(ch)
		s.Next()
	}

	bs := sb.String()
	_, err := humanize.ParseBytes(bs)
	return bs, err
}

func isBytesRune(r rune) bool {
	switch r {
	case 'b', 'B', 'i', 'k', 'K', 'M', 'g', 'G', 't', 'T', 'p', 'P':
		return true
	default:
		return false
	}
}

func scanFlag(s *scanner.Scanner, prefix string) string {
	var sb strings.Builder
	sb.WriteString(prefix)

	for {
		ch := s.Peek()
		if !unicode.IsLetter(ch) && ch != '-' {
			break
		}
		sb.WriteRune(ch)
		s.Next()
	}
	return sb.String()
}
