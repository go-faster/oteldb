// Package chsql provides fluent Clickhouse SQL query builder.
package chsql

import (
	"strings"

	"github.com/go-faster/errors"
)

// Printer prints SQL query.
type Printer struct {
	sb        strings.Builder
	needSpace bool
}

// GetPrinter creates a new [Printer].
func GetPrinter() *Printer {
	return new(Printer)
}

// String returns query.
func (p *Printer) String() string {
	return p.sb.String()
}

func (p *Printer) maybeSpace() {
	if p.needSpace {
		p.sb.WriteByte(' ')
		p.needSpace = false
	}
}

func (p *Printer) maybeNewline() {
	if p.needSpace {
		p.sb.WriteByte('\n')
		p.needSpace = false
	}
}

// Comma writes a comma.
func (p *Printer) Comma() {
	p.needSpace = false
	p.sb.WriteByte(',')
}

// OpenParen writes a paren.
func (p *Printer) OpenParen() {
	p.maybeSpace()
	p.sb.WriteByte('(')
}

// CloseParen writes a paren.
func (p *Printer) CloseParen() {
	p.sb.WriteByte(')')
	p.needSpace = true
}

// Ident writes an identifier.
func (p *Printer) Ident(tok string) {
	p.maybeSpace()
	p.sb.WriteString(tok)
	p.needSpace = true
}

// Literal writes an literal.
func (p *Printer) Literal(lit string) {
	p.maybeSpace()
	p.sb.WriteString(lit)
	p.needSpace = true
}

func (p *Printer) WriteExpr(e expr) error {
	switch e.typ {
	case exprIdent:
		p.Ident(e.tok)

		return nil
	case exprLiteral:
		p.Literal(e.tok)

		return nil
	case exprUnaryOp:
		if l := len(e.args); l != 1 {
			return errors.Errorf("unary expression must have exacty one arg, got %d", l)
		}

		p.Ident(e.tok)
		p.OpenParen()
		if err := p.WriteExpr(e.args[0]); err != nil {
			return err
		}
		p.CloseParen()

		return nil
	case exprBinaryOp:
		if l := len(e.args); l != 2 {
			return errors.Errorf("binary expression must have exacty two args, got %d", l)
		}

		if err := p.WriteExpr(e.args[0]); err != nil {
			return err
		}
		p.Ident(e.tok)
		if err := p.WriteExpr(e.args[1]); err != nil {
			return err
		}

		return nil
	case exprFunction:
		p.Ident(e.tok)
		p.needSpace = false
		p.OpenParen()
		for i, arg := range e.args {
			if i != 0 {
				p.sb.WriteByte(',')
			}
			if err := p.WriteExpr(arg); err != nil {
				return err
			}
		}
		p.CloseParen()

		return nil
	case exprTuple:
		p.OpenParen()
		for i, arg := range e.args {
			if i != 0 {
				p.sb.WriteByte(',')
			}
			if err := p.WriteExpr(arg); err != nil {
				return err
			}
		}
		p.CloseParen()

		return nil
	default:
		return errors.Errorf("unexpected expression type %v", e.typ)
	}
}

// And writes `AND` ident.
func (p *Printer) And() {
	p.Ident("AND")
}

// Select writes `SELECT` ident.
func (p *Printer) Select() {
	p.Ident("SELECT")
}

// Distinct writes `DISTINCT` ident.
func (p *Printer) Distinct() {
	p.Ident("DISTINCT")
}

// From writes `FROM` ident.
func (p *Printer) From() {
	p.Ident("FROM")
}

// Where writes `WHERE` ident.
func (p *Printer) Where() {
	p.Ident("WHERE")
}

// Limit writes `LIMIT` ident.
func (p *Printer) Limit() {
	p.Ident("LIMIT")
}
