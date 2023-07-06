package logqlengine

// Matcher is a generic matcher.
type Matcher[T any] interface {
	Match(T) bool
}

// NotMatcher is a NOT logical matcher.
type NotMatcher[T any, M Matcher[T]] struct {
	Next M
}

// Match implements StringMatcher.
func (m NotMatcher[T, M]) Match(v T) bool {
	return !m.Next.Match(v)
}
