package traceql

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/internal/traceql/lexer"
)

func (p *parser) parseFieldExpr() (FieldExpr, error) {
	expr, err := p.parseFieldExpr1()
	if err != nil {
		return nil, err
	}
	return p.parseBinaryFieldExpr(expr, 0)
}

func (p *parser) parseFieldExpr1() (FieldExpr, error) {
	switch t := p.peek(); t.Type {
	case lexer.OpenParen:
		p.next()

		expr, err := p.parseFieldExpr()
		if err != nil {
			return nil, err
		}

		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}
		return expr, nil
	case lexer.Not, lexer.Sub:
		p.next()

		pos := p.peek().Pos
		expr, err := p.parseFieldExpr1()
		if err != nil {
			return nil, err
		}

		var op UnaryOp
		switch t.Type {
		case lexer.Not:
			op = OpNot
		case lexer.Sub:
			op = OpNeg
		}

		if t := expr.ValueType(); !op.CheckType(t) {
			return nil, &TypeError{
				Msg: fmt.Sprintf("unary operator %q not defined on %q", op, t),
				Pos: pos,
			}
		}
		return &UnaryFieldExpr{
			Expr: expr,
			Op:   op,
		}, nil
	default:
		switch s, ok, err := p.tryStatic(); {
		case err != nil:
			return nil, err
		case ok:
			return s, nil
		}

		if a, ok := p.tryAttribute(); ok {
			return &a, nil
		}
		return nil, p.unexpectedToken(t)
	}
}

func (p *parser) parseBinaryFieldExpr(left FieldExpr, minPrecedence int) (FieldExpr, error) {
	for {
		op, ok := p.peekBinaryOp()
		if !ok || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op and get op token position.
		opPos := p.next().Pos

		// Get right expression position.
		rightPos := p.peek().Pos
		right, err := p.parseFieldExpr1()
		if err != nil {
			return nil, err
		}

		if op.IsRegex() {
			if _, ok := right.(*Static); !ok {
				return nil, &TypeError{
					Msg: fmt.Sprintf("regexp pattern should be a static string, got %T", right),
					Pos: rightPos,
				}
			}
		}

		for {
			rightOp, ok := p.peekBinaryOp()
			if !ok || rightOp.Precedence() < op.Precedence() {
				break
			}

			nextPrecedence := op.Precedence()
			if rightOp.Precedence() > op.Precedence() {
				nextPrecedence++
			}

			right, err = p.parseBinaryFieldExpr(right, nextPrecedence)
			if err != nil {
				return nil, err
			}
		}

		if err := p.checkBinaryExpr(left, op, opPos, right, rightPos); err != nil {
			return nil, err
		}
		left = &BinaryFieldExpr{Left: left, Op: op, Right: right}
	}
}

func (p *parser) peekBinaryOp() (op BinaryOp, _ bool) {
	switch t := p.peek(); t.Type {
	case lexer.Eq:
		return OpEq, true
	case lexer.NotEq:
		return OpNotEq, true
	case lexer.Re:
		return OpRe, true
	case lexer.NotRe:
		return OpNotRe, true
	case lexer.Gt:
		return OpGt, true
	case lexer.Gte:
		return OpGte, true
	case lexer.Lt:
		return OpLt, true
	case lexer.Lte:
		return OpLte, true
	case lexer.Add:
		return OpAdd, true
	case lexer.Sub:
		return OpSub, true
	case lexer.Div:
		return OpDiv, true
	case lexer.Mod:
		return OpMod, true
	case lexer.Mul:
		return OpMul, true
	case lexer.Pow:
		return OpPow, true
	case lexer.And:
		return OpAnd, true
	case lexer.Or:
		return OpOr, true
	default:
		return op, false
	}
}

func (p *parser) parseStatic() (*Static, error) {
	switch s, ok, err := p.tryStatic(); {
	case err != nil:
		return nil, err
	case ok:
		return s, nil
	default:
		return nil, p.unexpectedToken(p.peek())
	}
}

func (p *parser) tryStatic() (s *Static, ok bool, _ error) {
	s = new(Static)
	switch t := p.peek(); t.Type {
	case lexer.String:
		p.next()
		s.SetString(t.Text)
	case lexer.Integer:
		v, err := p.parseInteger()
		if err != nil {
			return s, false, err
		}
		s.SetInt(v)
	case lexer.Number:
		v, err := p.parseNumber()
		if err != nil {
			return s, false, err
		}
		s.SetNumber(v)
	case lexer.True:
		p.next()
		s.SetBool(true)
	case lexer.False:
		p.next()
		s.SetBool(false)
	case lexer.Nil:
		p.next()
		s.SetNil()
	case lexer.Duration:
		v, err := p.parseDuration()
		if err != nil {
			return s, false, err
		}
		s.SetDuration(v)
	case lexer.StatusOk:
		p.next()
		s.SetSpanStatus(ptrace.StatusCodeOk)
	case lexer.StatusError:
		p.next()
		s.SetSpanStatus(ptrace.StatusCodeError)
	case lexer.StatusUnset:
		p.next()
		s.SetSpanStatus(ptrace.StatusCodeUnset)
	case lexer.KindUnspecified:
		p.next()
		s.SetSpanKind(ptrace.SpanKindUnspecified)
	case lexer.KindInternal:
		p.next()
		s.SetSpanKind(ptrace.SpanKindInternal)
	case lexer.KindServer:
		p.next()
		s.SetSpanKind(ptrace.SpanKindServer)
	case lexer.KindClient:
		p.next()
		s.SetSpanKind(ptrace.SpanKindClient)
	case lexer.KindProducer:
		p.next()
		s.SetSpanKind(ptrace.SpanKindProducer)
	case lexer.KindConsumer:
		p.next()
		s.SetSpanKind(ptrace.SpanKindConsumer)
	default:
		return s, false, nil
	}
	return s, true, nil
}

func (p *parser) tryAttribute() (a Attribute, _ bool) {
	switch t := p.peek(); t.Type {
	case lexer.SpanDuration:
		a.Prop = SpanDuration
	case lexer.ChildCount:
		a.Prop = SpanChildCount
	case lexer.Name:
		a.Prop = SpanName
	case lexer.Status:
		a.Prop = SpanStatus
	case lexer.Kind:
		a.Prop = SpanKind
	case lexer.Parent:
		a.Prop = SpanParent
	case lexer.RootName:
		a.Prop = RootSpanName
	case lexer.RootServiceName:
		a.Prop = RootServiceName
	case lexer.TraceDuration:
		a.Prop = TraceDuration
	case lexer.Ident:
		parseAttributeSelector(t.Text, &a)
	default:
		return a, false
	}
	p.next()

	return a, true
}

func parseAttributeSelector(attr string, a *Attribute) {
	attr, a.Parent = strings.CutPrefix(attr, "parent.")

	uncut := attr
	scope, attr, ok := strings.Cut(attr, ".")
	if !ok {
		a.Name = uncut
		return
	}

	switch scope {
	case "resource":
		a.Name = attr
		a.Scope = ScopeResource
	case "span":
		a.Name = attr
		a.Scope = ScopeSpan
	case "":
		a.Name = attr
		a.Scope = ScopeNone
	default:
		a.Name = uncut
		a.Scope = ScopeNone
	}
}
