// Package logqlpattern contains parser for LogQL `pattern` stage pattern.
package logqlpattern

import (
	"io"
	"strings"
	"text/scanner"
	"unicode/utf8"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/lexerql"
)

// PartType defines type of [Part].
type PartType uint8

const (
	Capture PartType = iota + 1
	Literal
)

// Part is a pattern part.
type Part struct {
	Type  PartType
	Value string
}

// Pattern is a parsed pattern.
type Pattern struct {
	Parts []Part
}

// ParseFlags defines options for [Parse].
type ParseFlags uint8

// Has whether if flag is set.
func (f ParseFlags) Has(flag ParseFlags) bool {
	return f&flag != 0
}

const (
	RequireCapture ParseFlags = 1 << iota
	DisallowNamed

	ExtractorFlags  = RequireCapture
	LineFilterFlags = DisallowNamed
)

// Parse parses pattern.
func Parse(input string, flags ParseFlags) (p Pattern, _ error) {
	r := &reader{
		input: input,
	}

	var captures int
scanLoop:
	for {
		part, err := r.Scan()
		switch {
		case err == nil:
			if part.Type == Capture {
				captures++
			}
			p.Parts = append(p.Parts, part)
		case err == io.EOF:
			break scanLoop
		default:
			return p, err
		}
	}
	if flags.Has(RequireCapture) {
		if captures < 1 {
			return p, errors.New("at least one capture is expected")
		}
	}

	if flags.Has(DisallowNamed) {
		for _, part := range p.Parts {
			if part.Type == Capture && part.Value != "_" {
				return p, errors.Errorf("unexpected named pattern %q", part.Value)
			}
		}
	} else {
		dedup := make(map[string]struct{}, captures)
		for _, part := range p.Parts {
			if part.Type != Capture || part.Value == "_" {
				continue
			}
			if _, ok := dedup[part.Value]; ok {
				return p, errors.Errorf("duplicate capture %q", part.Value)
			}
			dedup[part.Value] = struct{}{}
		}
	}

	for i, part := range p.Parts {
		if i+1 >= len(p.Parts) {
			break
		}
		next := p.Parts[i+1]

		if part.Type == Capture && next.Type == Capture {
			return p, errors.Errorf(
				"consecutive capture: literal expected between <%s> and <%s>",
				part.Value, next.Value,
			)
		}
	}

	return p, nil
}

type reader struct {
	input string
	pos   int
}

func (r *reader) Scan() (Part, error) {
	ch := r.Peek()
	switch ch {
	case scanner.EOF:
		return Part{}, io.EOF
	case '<':
		// Consume '<'.
		r.Read()
		return r.scanCapture()
	default:
		return r.scanLiteral("")
	}
}

// scanCapture scans Capture.
//
// precondition: caller must read '<'.
func (r *reader) scanCapture() (Part, error) {
	// Label should start with `[_A-Za-z]`.
	// If it do not, consider part as literal.
	if ch := r.Peek(); !lexerql.IsIdentStartRune(ch) {
		return r.scanLiteral("<")
	}

	var label strings.Builder
	label.WriteString("<")
	for {
		switch ch := r.Peek(); ch {
		case scanner.EOF:
			// Got tail literal starting with '<'.
			return Part{
				Type:  Literal,
				Value: label.String(),
			}, nil
		case '>':
			// Consume '>'.
			r.Read()
			return Part{
				Type: Capture,
				// Trim leading '<'.
				Value: strings.TrimPrefix(label.String(), "<"),
			}, nil
		default:
			if lexerql.IsIdentRune(ch) {
				label.WriteRune(r.Read())
			} else {
				return r.scanLiteral(label.String())
			}
		}
	}
}

func (r *reader) scanLiteral(prefix string) (Part, error) {
	var literal strings.Builder
	literal.WriteString(prefix)
	for {
		switch ch := r.Peek(); ch {
		case scanner.EOF:
			return Part{
				Type:  Literal,
				Value: literal.String(),
			}, nil
		case '<':
			// Consume '<'.
			r.Read()
			// Label should start with `[_A-Za-z]`.
			if ch := r.Peek(); !lexerql.IsIdentStartRune(ch) {
				literal.WriteString("<")
				continue
			}
			r.Unread()
			return Part{
				Type:  Literal,
				Value: literal.String(),
			}, nil
		default:
			literal.WriteRune(r.Read())
		}
	}
}

func (r *reader) next() (ch rune, size int) {
	if r.pos >= len(r.input) {
		return scanner.EOF, 0
	}
	return utf8.DecodeRuneInString(r.input[r.pos:])
}

func (r *reader) Unread() {
	if r.pos > 0 {
		r.pos--
	}
}

func (r *reader) Peek() rune {
	ch, _ := r.next()
	return ch
}

func (r *reader) Read() rune {
	ch, size := r.next()
	r.pos += size
	return ch
}
