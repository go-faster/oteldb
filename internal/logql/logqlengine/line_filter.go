package logqlengine

import (
	"fmt"
	"net/netip"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlerrors"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func buildLineFilter(stage *logql.LineFilter) (Processor, error) {
	switch op := stage.Op; op {
	case logql.OpPattern, logql.OpNotPattern:
		return nil, &logqlerrors.UnsupportedError{Msg: fmt.Sprintf("%s line filter is unsupported", op)}
	}
	if len(stage.Or) > 0 {
		return nil, &logqlerrors.UnsupportedError{Msg: "or in line filters is unsupported"}
	}

	if stage.By.IP {
		matcher, err := buildIPMatcher(stage.Op, stage.By.Value)
		if err != nil {
			return nil, err
		}

		return &IPLineFilter{matcher: matcher}, nil
	}

	matcher, err := buildStringMatcher(stage.Op, stage.By.Value, stage.By.Re, false)
	if err != nil {
		return nil, err
	}

	return &LineFilter{matcher: matcher}, nil
}

// LineFilter is a line matching Processor.
type LineFilter struct {
	matcher StringMatcher
}

// Process implements Processor.
func (lf *LineFilter) Process(_ otelstorage.Timestamp, line string, _ logqlabels.LabelSet) (_ string, keep bool) {
	keep = lf.matcher.Match(line)
	return line, keep
}

// IPLineFilter looks for IP address in a line and applies matcher to it.
type IPLineFilter struct {
	matcher IPMatcher
}

// Process implements Processor.
func (lf *IPLineFilter) Process(_ otelstorage.Timestamp, line string, _ logqlabels.LabelSet) (_ string, keep bool) {
	for i := 0; i < len(line); {
		c := line[i]
		if !isHexDigit(c) && c != ':' {
			i++
			continue
		}

		if capture, ok := tryCaptureIPv4(line[i:]); ok {
			i += len(capture)

			ip, err := netip.ParseAddr(capture)
			if err == nil && lf.matcher.Match(ip) {
				return line, true
			}
			continue
		}
		if capture, ok := tryCaptureIPv6(line[i:]); ok {
			i += len(capture)

			ip, err := netip.ParseAddr(capture)
			if err == nil && lf.matcher.Match(ip) {
				return line, true
			}
			continue
		}
		i++
	}

	return line, false
}

func tryCaptureIPv4(s string) (string, bool) {
	if len(s) < 4 || !isDigit(s[0]) {
		return "", false
	}

	switch byte('.') {
	case s[1], s[2], s[3]:
	default:
		return "", false
	}

	for i, c := range []byte(s) {
		if !isDigit(c) && c != '.' {
			s = s[:i]
			break
		}
	}

	return s, true
}

func tryCaptureIPv6(s string) (string, bool) {
	if len(s) < 2 {
		return "", false
	}

	switch {
	case s[0] == ':' && s[1] == ':':
		// ::1
	case isHexDigit(s[0]):
		for _, c := range []byte(s[1:]) {
			switch {
			case isHexDigit(c):
				continue
			case c == ':':
				goto match
			}
		}
		return "", false
	default:
		return "", false
	}

match:
	for i, c := range []byte(s) {
		if !isHexDigit(c) && c != ':' {
			s = s[:i]
			break
		}
	}

	return s, true
}

func isHexDigit(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}
