package traceql

import "github.com/go-faster/oteldb/internal/traceql/lexer"

// Autocomplete is a AND set of spanset matchers.
type Autocomplete struct {
	Matchers []BinaryFieldExpr
}

// ParseAutocomplete parses matchers from potentially uncomplete TraceQL spanset filter from string.
func ParseAutocomplete(input string) (c Autocomplete) {
	p, err := newParser(input)
	if err != nil {
		return c
	}

	if err := p.consume(lexer.OpenBrace); err != nil {
		return c
	}

	for {
		left, ok, err := parseSimpleFieldExpr(&p)
		if err != nil || !ok {
			return c
		}

		op, ok := p.peekBinaryOp()
		if !ok || !(op.IsOrdering() || op.IsRegex()) {
			return c
		}
		// Consume op.
		p.next()

		right, ok, err := parseSimpleFieldExpr(&p)
		switch {
		case err != nil:
			return c
		case !ok:
			// Handle cases like `{ .foo = <missing field> && .bar = 10 }`.
			op, ok := p.peekBinaryOp()
			if !ok {
				return c
			}
			if op != OpAnd {
				return Autocomplete{}
			}
		default:
			c.Matchers = append(c.Matchers, BinaryFieldExpr{
				Left:  left,
				Op:    op,
				Right: right,
			})
		}

		switch t := p.peek(); t.Type {
		case lexer.EOF:
			return c
		case lexer.CloseBrace:
			p.next()
			return c
		default:
			op, ok := p.peekBinaryOp()
			if !ok {
				return c
			}
			if op != OpAnd {
				return Autocomplete{}
			}
			// Consume op.
			p.next()
		}
	}
}

func parseSimpleFieldExpr(p *parser) (FieldExpr, bool, error) {
	switch s, ok, err := p.tryStatic(); {
	case err != nil:
		return nil, false, err
	case ok:
		return s, true, nil
	}

	switch a, ok, err := p.tryAttribute(); {
	case err != nil:
		return nil, false, err
	case ok:
		return a, true, nil
	}

	return nil, false, nil
}
