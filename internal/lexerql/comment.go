package lexerql

import "text/scanner"

// ScanComment reads runes until newline.
func ScanComment(s *scanner.Scanner) {
	for {
		ch := s.Next()
		if ch == scanner.EOF || ch == '\n' {
			break
		}
	}
}
