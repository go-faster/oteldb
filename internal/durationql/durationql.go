// Package durationql provides utilities to parse duration in *QL languages.
package durationql

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
		if !isDigit(ch) && !IsDurationRune(ch) && ch != '.' {
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
func IsDurationRune(r rune) bool {
	switch r {
	case 'n', 'u', 'µ', 'm', 's', 'h', 'd', 'w', 'y':
		return true
	default:
		return false
	}
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
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
