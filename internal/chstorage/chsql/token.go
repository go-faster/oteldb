package chsql

// IsSingleToken whether if given string is a single token.
//
// See https://clickhouse.com/docs/en/sql-reference/functions/string-search-functions#hastoken.
// See https://github.com/ClickHouse/ClickHouse/blob/755b73f3fc99847f40ac4d9186bb19116e709c37/src/Interpreters/ITokenExtractor.cpp#L84.
func IsSingleToken[S ~string | ~[]byte](s S) bool {
	if len(s) == 0 {
		return false
	}
	// If string does contain any non-alphanumeric ASCII characters.
	// then it is not a single token.
	for _, c := range []byte(s) {
		if isTokenSeparator(c) {
			return false
		}
	}
	return true
}

// CollectTokens iterates over tokens in given string.
func CollectTokens[S ~string | ~[]byte](s S, cb func(s S) bool) {
	// FIXME(tdakkota): use go1.23 iterators.
	if len(s) == 0 {
		return
	}
	// If string does contain any non-alphanumeric ASCII characters.
	// then it is not a single token.
	var (
		i, lastIdx int
		c          byte
	)
	for i, c = range []byte(s) {
		if !isTokenSeparator(c) {
			continue
		}
		tok := s[lastIdx:i]
		if len(tok) > 0 && !cb(tok) {
			return
		}
		lastIdx = i + 1
	}
	if tok := s[lastIdx:]; len(tok) > 0 {
		cb(s[lastIdx:])
	}
}

func isTokenSeparator(c byte) bool {
	return c < 0x80 && !isAlphaNumeric(c)
}

func isAlphaNumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}
