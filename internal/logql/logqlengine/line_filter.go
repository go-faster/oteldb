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
	matcher, err := buildLineMatcher(stage.Op, stage.By)
	if err != nil {
		return nil, err
	}

	if len(stage.Or) > 0 {
		matchers := make([]StringMatcher, 0, len(stage.Or)+1)
		matchers = append(matchers, matcher)

		for _, by := range stage.Or {
			m, err := buildLineMatcher(stage.Op, by)
			if err != nil {
				return nil, err
			}
			matchers = append(matchers, m)
		}
		return &OrLineFilter{matchers: matchers}, nil
	}

	return &LineFilter{matcher: matcher}, nil
}

func buildLineMatcher(op logql.BinOp, by logql.LineFilterValue) (StringMatcher, error) {
	switch op {
	case logql.OpPattern, logql.OpNotPattern:
		return nil, &logqlerrors.UnsupportedError{Msg: fmt.Sprintf("%s line filter is unsupported", op)}
	}

	if by.IP {
		matcher, err := buildIPMatcher(op, by.Value)
		if err != nil {
			return nil, err
		}
		return &IPLineMatcher{matcher: matcher}, nil
	}
	return buildStringMatcher(op, by.Value, by.Re, false)
}

// OrLineFilter is a line matching Processor.
type OrLineFilter struct {
	matchers []StringMatcher
}

// Process implements Processor.
func (lf *OrLineFilter) Process(_ otelstorage.Timestamp, line string, _ logqlabels.LabelSet) (_ string, keep bool) {
	// TODO(tdakkota): cache IP captures
	for _, m := range lf.matchers {
		if m.Match(line) {
			return line, true
		}
	}
	return line, false
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

// IPLineMatcher looks for IP address in a line and applies matcher to it.
type IPLineMatcher struct {
	matcher IPMatcher
}

var _ StringMatcher = (*IPLineMatcher)(nil)

// Match implements StringMatcher.
func (lf *IPLineMatcher) Match(line string) bool {
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
				return true
			}
			continue
		}
		if capture, ok := tryCaptureIPv6(line[i:]); ok {
			i += len(capture)

			ip, err := netip.ParseAddr(capture)
			if err == nil && lf.matcher.Match(ip) {
				return true
			}
			continue
		}
		i++
	}

	return false
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
