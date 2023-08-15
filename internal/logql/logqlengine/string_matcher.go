package logqlengine

import (
	"regexp"
	"strings"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql"
)

// StringMatcher matches a string.
type StringMatcher interface {
	Matcher[string]
}

func buildStringMatcher(op logql.BinOp, value string, re *regexp.Regexp, label bool) (m StringMatcher, _ error) {
	switch op {
	case logql.OpRe, logql.OpNotRe:
		if re == nil {
			return m, errors.Errorf("internal error: regexp %q is not compiled", value)
		}
	}

	switch op {
	case logql.OpEq:
		if label {
			m = EqualsMatcher{Value: value}
		} else {
			m = ContainsMatcher{Value: value}
		}
	case logql.OpNotEq:
		if label {
			m = NotMatcher[string, EqualsMatcher]{Next: EqualsMatcher{Value: value}}
		} else {
			m = NotMatcher[string, ContainsMatcher]{Next: ContainsMatcher{Value: value}}
		}
	case logql.OpRe:
		// TODO(tdakkota): optimize regexp.
		m = RegexpMatcher{Re: re}
	case logql.OpNotRe:
		m = NotMatcher[string, RegexpMatcher]{Next: RegexpMatcher{Re: re}}
	default:
		return nil, errors.Errorf("unexpected operation %q", op)
	}
	return m, nil
}

// ContainsMatcher checks if a string contains value.
type ContainsMatcher struct {
	Value string
}

// Match implements StringMatcher.
func (m ContainsMatcher) Match(s string) bool {
	return strings.Contains(s, m.Value)
}

// EqualsMatcher checks if a string equals to a value.
type EqualsMatcher struct {
	Value string
}

// Match implements StringMatcher.
func (m EqualsMatcher) Match(s string) bool {
	return s == m.Value
}

// RegexpMatcher checks if a matches regular expression.
type RegexpMatcher struct {
	Re *regexp.Regexp
}

// Match implements StringMatcher.
func (m RegexpMatcher) Match(s string) bool {
	return m.Re.MatchString(s)
}
