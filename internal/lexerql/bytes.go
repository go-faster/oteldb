package lexerql

// IsBytesRune returns true, if r is a non-digit rune that could be part of bytes.
func IsBytesRune[R char](r R) bool {
	switch r {
	case 'b', 'B', 'i', 'k', 'K', 'M', 'g', 'G', 't', 'T', 'p', 'P':
		return true
	default:
		return false
	}
}
