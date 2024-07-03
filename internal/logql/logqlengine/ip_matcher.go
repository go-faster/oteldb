package logqlengine

import (
	"net/netip"
	"strings"

	"github.com/go-faster/errors"
	"go4.org/netipx"
)

// IPMatcher matches an IP.
type IPMatcher interface {
	Matcher[netip.Addr]
}

func buildIPMatcher(pattern string) (m IPMatcher, _ error) {
	switch {
	case strings.Contains(pattern, "-"):
		if ipRange, err := netipx.ParseIPRange(pattern); err == nil {
			return RangeIPMatcher{Range: ipRange}, nil
		}
	case strings.Contains(pattern, "/"):
		if prefix, err := netip.ParsePrefix(pattern); err == nil {
			return PrefixIPMatcher{Prefix: prefix}, nil
		}
	}

	addr, err := netip.ParseAddr(pattern)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid addr %q", pattern)
	}
	return EqualIPMatcher{Value: addr}, nil
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
