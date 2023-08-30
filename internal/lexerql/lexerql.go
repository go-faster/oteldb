package lexerql

type char interface {
	byte | rune
}

// IsDigit returns true, if r is an ASCII digit.
func IsDigit[R char](r R) bool {
	return r >= '0' && r <= '9'
}

// IsLetter returns true, if r is an ASCII letter.
func IsLetter[R char](r R) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z')
}

// IsIdentStartRune returns true, if r is a valid first character of Go identifier/Prometheus label.
func IsIdentStartRune[R char](r R) bool {
	return IsLetter(r) || r == '_'
}

// IsIdentRune returns true, if r is a valid character of Go identifier/Prometheus label.
func IsIdentRune[R char](r R) bool {
	return IsLetter(r) || IsDigit(r) || r == '_'
}
