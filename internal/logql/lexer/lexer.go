// Package lexer contains LogQL lexer.
package lexer

import (
	"strings"
	"text/scanner"
	"time"
	"unicode"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/util/strutil"
)

type lexer struct {
	scanner scanner.Scanner
	tokens  []Token
	err     error
}

// Tokenize scans given string to LogQL tokens.
func Tokenize(s string) ([]Token, error) {
	l := lexer{}
	l.scanner.Init(strings.NewReader(s))
	l.scanner.IsIdentRune = func(ch rune, i int) bool {
		return ch == '_' || unicode.IsLetter(ch) ||
			(unicode.IsDigit(ch) && i > 0) ||
			(ch == '.' && i > 0) // allow dot if it is not the first character of token
	}
	l.scanner.Error = func(s *scanner.Scanner, msg string) {
		l.err = errors.Errorf("scanner error: %s", msg)
	}

	for {
		r := l.scanner.Scan()
		if r == scanner.EOF {
			return l.tokens, l.err
		}

		tok, err := l.nextToken(r, l.scanner.TokenText())
		if err != nil {
			return l.tokens, err
		}
		l.tokens = append(l.tokens, tok)
	}
}

func (l *lexer) nextToken(r rune, text string) (tok Token, err error) {
	tok.Text = text
	switch r {
	case scanner.Int, scanner.Float:
		switch r := l.scanner.Peek(); {
		case isDurationRune(r):
			tok.Type = Duration
			tok.Text, err = scanDuration(&l.scanner, text)
		case isBytesRune(r):
			tok.Type = Bytes
			tok.Text, err = scanBytes(&l.scanner, text)
		default:
			tok.Type = Number
		}
		return tok, err
	case scanner.String, scanner.RawString:
		tok.Type = String
		// FIXME(tdakkota): requires a huge dependency
		tok.Text, err = strutil.Unquote(text)
		return tok, err
	}
	peekCh := l.scanner.Peek()
	peeked := text + string(peekCh)

	tt, ok := tokens[peeked]
	if ok {
		l.scanner.Next()
		tok.Type = tt
		tok.Text = peeked
		return tok, nil
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
		return tok, nil
	}

	tok.Type = Ident
	return tok, nil
}

func scanSpace(s *scanner.Scanner) {
	for {
		if ch := s.Peek(); !unicode.IsSpace(ch) {
			return
		}
		s.Next()
	}
}

func scanDuration(s *scanner.Scanner, number string) (string, error) {
	var sb strings.Builder
	sb.WriteString(number)

	for {
		ch := s.Peek()
		if !isDigit(ch) && !isDurationRune(ch) && ch != '.' {
			break
		}
		sb.WriteRune(ch)
		s.Next()
	}

	duration := sb.String()
	_, err := ParseDuration(duration)
	return duration, err
}

func isDurationRune(r rune) bool {
	switch r {
	case 'n', 'u', 'µ', 'm', 's', 'h', 'd', 'w', 'y':
		return true
	default:
		return false
	}
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func scanBytes(s *scanner.Scanner, number string) (string, error) {
	var sb strings.Builder
	sb.WriteString(number)

	for {
		ch := s.Peek()
		if !isDigit(ch) && !isBytesRune(ch) && ch != '.' {
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

// ParseDuration parses Prometheus or Go duration
func ParseDuration(s string) (time.Duration, error) {
	d, err := model.ParseDuration(s)
	if err == nil {
		return time.Duration(d), nil
	}
	err1 := err

	d2, err := time.ParseDuration(s)
	if err == nil {
		return d2, nil
	}
	return 0, err1
}
