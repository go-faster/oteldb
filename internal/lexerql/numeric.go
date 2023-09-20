package lexerql

import (
	"strings"
	"text/scanner"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
)

// UnitType defines [Unit] type.
type UnitType int

const (
	// Number is a plain Go number.
	Number = iota
	// Duration is a Prometheus/Go duration.
	Duration
	// Bytes is a [humanize.Bytes] value.
	Bytes
)

// Unit represents suffixed numeric token like duration.
type Unit struct {
	Type UnitType
	Text string
}

// ScanUnit scans unit tokens like durations and bytes.
func ScanUnit(s *scanner.Scanner, prefix string) (Unit, error) {
	if !isValueRune(s.Peek()) {
		return Unit{Type: Number, Text: prefix}, nil
	}

	var sb strings.Builder
	sb.WriteString(prefix)

	suffixStart := sb.Len()
	var unit string

	for {
		ch := s.Peek()

		suffixRune := isUnitRune(ch)
		if !suffixRune && unit == "" {
			suffixEnd := sb.Len()
			unit = sb.String()[suffixStart:suffixEnd]
		}

		if !isValueRune(ch) {
			break
		}
		sb.WriteRune(ch)
		s.Next()
	}

	text := sb.String()
	switch strings.ToLower(unit) {
	case "b",
		"kib",
		"kb",
		"mib",
		"mb",
		"gib",
		"gb",
		"tib",
		"tb",
		"pib",
		"pb",
		"eib",
		"eb",
		"ki",
		"k",
		"mi",
		// "m",
		"gi",
		"g",
		"ti",
		"t",
		"pi",
		"p",
		"ei",
		"e":
		_, err := humanize.ParseBytes(text)
		return Unit{Type: Bytes, Text: text}, err
	case "ns",
		"us",
		"µs",
		"μs",
		"ms",
		"s",
		"m",
		"h",
		"d",
		"w":
		_, err := ParseDuration(text)
		return Unit{Type: Duration, Text: text}, err
	default:
		return Unit{}, errors.Errorf("unknown unit %q", unit)
	}
}

func isValueRune[R char](ch R) bool {
	return IsDigit(ch) ||
		ch == '.' ||
		isUnitRune(ch)
}

func isUnitRune[R char](ch R) bool {
	return IsDurationRune(ch) || IsBytesRune(ch)
}
