package chstorage

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

func singleQuoted[S ~[]byte | ~string](s S) string {
	const lowerhex = "0123456789abcdef"

	var sb strings.Builder
	sb.Grow(len(s) + 2)
	sb.WriteByte('\'')
	for _, r := range string(s) {
		switch r {
		case '\'':
			sb.WriteString(`\'`)
		case '\\':
			sb.WriteString(`\\`)
		case '\a':
			sb.WriteString(`\a`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		case '\v':
			sb.WriteString(`\v`)
		default:
			if unicode.IsPrint(r) {
				sb.WriteRune(r)
				continue
			}
			switch {
			case r < ' ' || r == 0x7f:
				sb.WriteString(`\x`)
				sb.WriteByte(lowerhex[byte(r)>>4])
				sb.WriteByte(lowerhex[byte(r)&0xF])
			case !utf8.ValidRune(r):
				r = 0xFFFD
				fallthrough
			case r < 0x10000:
				sb.WriteString(`\u`)
				for s := 12; s >= 0; s -= 4 {
					sb.WriteByte(lowerhex[r>>uint(s)&0xF])
				}
			default:
				sb.WriteString(`\U`)
				for s := 28; s >= 0; s -= 4 {
					sb.WriteByte(lowerhex[r>>uint(s)&0xF])
				}
			}
		}
	}
	sb.WriteByte('\'')
	return sb.String()
}
