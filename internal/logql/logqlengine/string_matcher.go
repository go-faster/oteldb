package logqlengine

import (
	"regexp"
	"strings"
)

// StringMatcher matches a string.
type StringMatcher interface {
	Matcher[string]
}

// ContainsMatcher checks if a string contains value.
type ContainsMatcher struct {
	Value string
}

// Match implements StringMatcher.
func (m ContainsMatcher) Match(s string) bool {
	return strings.Contains(s, m.Value)
}

// RegexpMatcher checks if a matches regular expression.
type RegexpMatcher struct {
	Re *regexp.Regexp
}

// Match implements StringMatcher.
func (m RegexpMatcher) Match(s string) bool {
	return m.Re.MatchString(s)
}
