package traceql

import (
	"fmt"
	"strings"

	"github.com/go-faster/errors"
)

// ParseAttribute parses attribute from given string.
func ParseAttribute(attr string) (a Attribute, _ error) {
	p, err := newParser(attr)
	if err != nil {
		return a, err
	}

	a, ok := p.tryAttribute()
	if !ok {
		return a, errors.Errorf("invalid attribute %q", attr)
	}

	return a, nil
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

// ValueType returns value type of expression.
func (s *Attribute) ValueType() StaticType {
	switch s.Prop {
	case SpanDuration:
		return TypeDuration
	case SpanChildCount:
		return TypeInt
	case SpanName:
		return TypeString
	case SpanStatus:
		return TypeSpanStatus
	case SpanKind:
		return TypeSpanKind
	case SpanParent:
		return TypeNil
	case RootSpanName:
		return TypeString
	case RootServiceName:
		return TypeString
	case TraceDuration:
		return TypeDuration
	default:
		// Type determined at execution time.
		return TypeAttribute
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

var intrinsicNames = func() (r []string) {
	r = make([]string, 0, TraceDuration)
	for i := SpanDuration; i <= TraceDuration; i++ {
		r = append(r, Attribute{Prop: i}.String())
	}
	return r
}()

// IntrinsicNames returns a slice of intrinsics.
func IntrinsicNames() (r []string) {
	return intrinsicNames
}

// AttributeScope is an attribute scope.
type AttributeScope uint8

const (
	ScopeNone AttributeScope = iota
	ScopeResource
	ScopeSpan
	ScopeInstrumentation
)

// String implements [fmt.Stringer].
func (s AttributeScope) String() string {
	switch s {
	case ScopeNone:
		return "none"
	case ScopeResource:
		return "resource"
	case ScopeSpan:
		return "span"
	case ScopeInstrumentation:
		return "<instrumentation>"
	default:
		return fmt.Sprintf("unknown scope %d", uint8(s))
	}
}
