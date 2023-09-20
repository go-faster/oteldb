// Package lexerql provides utilities for lexing in *QL languages.
package lexerql

import (
	"strings"
	"text/scanner"
	"time"

	"github.com/prometheus/common/model"
)

// ScanDuration scans and parses duration from given scanner.
func ScanDuration(s *scanner.Scanner, number string) (string, error) {
	var sb strings.Builder
	sb.WriteString(number)

	for {
		ch := s.Peek()
		if !IsDigit(ch) && !IsDurationRune(ch) && ch != '.' {
			break
		}
		sb.WriteRune(ch)
		s.Next()
	}

	duration := sb.String()
	_, err := ParseDuration(duration)
	return duration, err
}

// IsDurationRune returns true, if r is a non-digit rune that could be part of duration.
func IsDurationRune[R char](r R) bool {
	switch rune(r) {
	case 'n', 'u', 'µ', 'μ', 'm', 's', 'h', 'd', 'w', 'y':
		return true
	default:
		return false
	}
}

// ParseDuration parses Prometheus or Go duration
func ParseDuration(s string) (time.Duration, error) {
	d, err := model.ParseDuration(s)
	if err == nil {
		return time.Duration(d), nil
	}
	err1 := err

	d2, err := time.ParseDuration(s)
	if err == nil {
		return d2, nil
	}
	return 0, err1
}
