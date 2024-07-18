package autometric

import (
	"strings"
	"unicode"
)

func snakeCase(s string) string {
	const delim = '_'
	s = strings.TrimSpace(s)
	for _, c := range s {
		if isUpper(c) {
			goto slow
		}
	}
	return s

slow:
	var sb strings.Builder
	sb.Grow(len(s) + 8)

	var prev, curr rune
	for i, next := range s {
		switch {
		case isDelim(curr):
			if !isDelim(prev) {
				sb.WriteByte(delim)
			}
		case isUpper(curr):
			if isLower(prev) ||
				(isUpper(prev) && isLower(next)) ||
				(isDigit(prev) && isAlpha(next)) {
				sb.WriteByte(delim)
			}
			sb.WriteRune(unicode.ToLower(curr))
		case i != 0:
			sb.WriteRune(unicode.ToLower(curr))
		}
		prev = curr
		curr = next
	}

	if s != "" {
		if isUpper(curr) && isLower(prev) {
			sb.WriteByte(delim)
		}
		sb.WriteRune(unicode.ToLower(curr))
	}

	return sb.String()
}

func isDelim(ch rune) bool {
	return unicode.IsSpace(ch) || ch == '_' || ch == '-'
}

func isAlpha(ch rune) bool {
	return isUpper(ch) || isLower(ch)
}

func isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
}

func isUpper(ch rune) bool {
	return ch >= 'A' && ch <= 'Z'
}

func isLower(ch rune) bool {
	return ch >= 'a' && ch <= 'z'
}
