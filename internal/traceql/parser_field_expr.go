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
	case lexer.String,
		lexer.Integer,
		lexer.Number,
		lexer.True,
		lexer.False,
		lexer.Nil,
		lexer.Duration,
		lexer.StatusOk,
		lexer.StatusError,
		lexer.StatusUnset,
		lexer.KindUnspecified,
		lexer.KindInternal,
		lexer.KindServer,
		lexer.KindClient,
		lexer.KindProducer,
		lexer.KindConsumer:
		return p.parseStatic()
	case lexer.SpanDuration,
		lexer.ChildCount,
		lexer.Name,
		lexer.Status,
		lexer.Kind,
		lexer.Parent,
		lexer.RootName,
		lexer.RootServiceName,
		lexer.TraceDuration,
		lexer.Ident:
		return p.parseAttribute()
	default:
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

			nextPrecedence := minPrecedence
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

func (p *parser) parseStatic() (s *Static, _ error) {
	s = new(Static)
	switch t := p.next(); t.Type {
	case lexer.String:
		s.SetString(t.Text)
	case lexer.Integer:
		p.unread()
		v, err := p.parseInteger()
		if err != nil {
			return s, err
		}
		s.SetInt(v)
	case lexer.Number:
		p.unread()
		v, err := p.parseNumber()
		if err != nil {
			return s, err
		}
		s.SetNumber(v)
	case lexer.True:
		s.SetBool(true)
	case lexer.False:
		s.SetBool(false)
	case lexer.Nil:
		s.SetNil()
	case lexer.Duration:
		p.unread()
		v, err := p.parseDuration()
		if err != nil {
			return s, err
		}
		s.SetDuration(v)
	case lexer.StatusOk:
		s.SetSpanStatus(ptrace.StatusCodeOk)
	case lexer.StatusError:
		s.SetSpanStatus(ptrace.StatusCodeError)
	case lexer.StatusUnset:
		s.SetSpanStatus(ptrace.StatusCodeUnset)
	case lexer.KindUnspecified:
		s.SetSpanKind(ptrace.SpanKindUnspecified)
	case lexer.KindInternal:
		s.SetSpanKind(ptrace.SpanKindInternal)
	case lexer.KindServer:
		s.SetSpanKind(ptrace.SpanKindServer)
	case lexer.KindClient:
		s.SetSpanKind(ptrace.SpanKindClient)
	case lexer.KindProducer:
		s.SetSpanKind(ptrace.SpanKindProducer)
	case lexer.KindConsumer:
		s.SetSpanKind(ptrace.SpanKindConsumer)
	default:
		return s, p.unexpectedToken(t)
	}
	return s, nil
}

func (p *parser) parseAttribute() (a *Attribute, _ error) {
	a = new(Attribute)
	switch t := p.next(); t.Type {
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
		attr := t.Text
		attr, a.Parent = strings.CutPrefix(attr, "parent.")

		uncut := attr
		scope, attr, ok := strings.Cut(attr, ".")
		if !ok {
			a.Name = uncut
		} else {
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
	default:
		return a, p.unexpectedToken(t)
	}

	return a, nil
}
