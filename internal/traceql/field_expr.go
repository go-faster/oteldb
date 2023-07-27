package traceql

import "strings"

// FieldExpr is a field expression.
type FieldExpr interface {
	fieldExpr()
}

func (*ParenFieldExpr) fieldExpr()  {}
func (*BinaryFieldExpr) fieldExpr() {}
func (*UnaryFieldExpr) fieldExpr()  {}
func (*Static) fieldExpr()          {}
func (*Attribute) fieldExpr()       {}

// ParenFieldExpr is a parenthesized field expression.
type ParenFieldExpr struct {
	Expr FieldExpr
}

// BinaryFieldExpr is a binary operation between two field expressions.
type BinaryFieldExpr struct {
	Left  FieldExpr
	Op    BinaryOp
	Right FieldExpr
}

// UnaryFieldExpr is a unary field expression operation.
type UnaryFieldExpr struct {
	Expr FieldExpr
	Op   UnaryOp
}

// Attribute is a span attribute.
type Attribute struct {
	Name   string
	Scope  AttributeScope
	Prop   SpanProperty
	Parent bool // refers to parent
}

// String implements fmt.Stringer.
func (s Attribute) String() string {
	switch s.Prop {
	case SpanDuration:
		return "duration"
	case SpanChildCount:
		return "childCount"
	case SpanName:
		return "name"
	case SpanStatus:
		return "status"
	case SpanKind:
		return "kind"
	case SpanParent:
		return "parent"
	case RootSpanName:
		return "rootName"
	case RootServiceName:
		return "rootServiceName"
	case TraceDuration:
		return "traceDuration"
	default:
		// SpanAttribute.
		var (
			sb      strings.Builder
			needDot = true
		)
		if s.Parent {
			sb.WriteString("parent.")
			needDot = false
		}
		switch s.Scope {
		case ScopeResource:
			sb.WriteString("resource")
			needDot = true
		case ScopeSpan:
			sb.WriteString("span")
			needDot = true
		}
		if needDot {
			sb.WriteByte('.')
		}
		sb.WriteString(s.Name)
		return sb.String()
	}
}

// SpanProperty is a span property.
type SpanProperty uint8

const (
	SpanAttribute SpanProperty = iota
	SpanDuration
	SpanChildCount
	SpanName
	SpanStatus
	SpanKind
	SpanParent
	RootSpanName
	RootServiceName
	TraceDuration
)

// AttributeScope is an attribute scope.
type AttributeScope uint8

const (
	ScopeNone AttributeScope = iota
	ScopeResource
	ScopeSpan
)
