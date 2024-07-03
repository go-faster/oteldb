package logqlengine

import (
	"net/netip"
	"strings"

	"github.com/go-faster/errors"
	"go4.org/netipx"

	"github.com/go-faster/oteldb/internal/logql"
)

// IPMatcher matches an IP.
type IPMatcher interface {
	Matcher[netip.Addr]
}

func buildIPMatcher(op logql.BinOp, pattern string) (m IPMatcher, _ error) {
	switch {
	case strings.Contains(pattern, "-"):
		ipRange, err := netipx.ParseIPRange(pattern)
		if err == nil {
			switch op {
			case logql.OpEq:
				return RangeIPMatcher{Range: ipRange}, nil
			case logql.OpNotEq:
				return NotMatcher[netip.Addr, RangeIPMatcher]{
					Next: RangeIPMatcher{Range: ipRange},
				}, nil
			default:
				return nil, errors.Errorf("unexpected operation %q", op)
			}
		}
	case strings.Contains(pattern, "/"):
		prefix, err := netip.ParsePrefix(pattern)
		if err == nil {
			switch op {
			case logql.OpEq:
				return PrefixIPMatcher{Prefix: prefix}, nil
			case logql.OpNotEq:
				return NotMatcher[netip.Addr, PrefixIPMatcher]{
					Next: PrefixIPMatcher{Prefix: prefix},
				}, nil
			default:
				return nil, errors.Errorf("unexpected operation %q", op)
			}
		}
	}

	addr, err := netip.ParseAddr(pattern)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid addr %q", pattern)
	}

	switch op {
	case logql.OpEq:
		return EqualIPMatcher{Value: addr}, nil
	case logql.OpNotEq:
		return NotMatcher[netip.Addr, EqualIPMatcher]{
			Next: EqualIPMatcher{Value: addr},
		}, nil
	default:
		return nil, errors.Errorf("unexpected operation %q", op)
	}
}

// EqualIPMatcher checks if an IP equal to given value.
type EqualIPMatcher struct {
	Value netip.Addr
}

// Match implements IPMatcher.
func (m EqualIPMatcher) Match(ip netip.Addr) bool {
	return m.Value.Compare(ip.Unmap()) == 0
}

// RangeIPMatcher checks if an IP is in given range.
type RangeIPMatcher struct {
	Range netipx.IPRange
}

// Match implements IPMatcher.
func (m RangeIPMatcher) Match(ip netip.Addr) bool {
	return m.Range.Contains(ip.Unmap())
}

// PrefixIPMatcher checks if an IP has given prefix.
type PrefixIPMatcher struct {
	Prefix netip.Prefix
}

// Match implements IPMatcher.
func (m PrefixIPMatcher) Match(ip netip.Addr) bool {
	return m.Prefix.Contains(ip.Unmap())
}
