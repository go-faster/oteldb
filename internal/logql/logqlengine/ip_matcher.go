package logqlengine

import (
	"net/netip"

	"go4.org/netipx"
)

// IPMatcher matches an IP.
type IPMatcher interface {
	Matcher[netip.Addr]
}

// EqualIPMatcher checks if an IP equal to given value.
type EqualIPMatcher struct {
	Value netip.Addr
}

// Match implements IPMatcher.
func (m EqualIPMatcher) Match(ip netip.Addr) bool {
	return m.Value.Compare(ip) == 0
}

// RangeIPMatcher checks if an IP is in given range.
type RangeIPMatcher struct {
	// FIXME(tdakkota): probably, it is better to just use two addrs
	// 	and compare them.
	Range netipx.IPRange
}

// Match implements IPMatcher.
func (m RangeIPMatcher) Match(ip netip.Addr) bool {
	return m.Range.Contains(ip)
}

// CIDRMatcher checks if an IP is in given range.
type CIDRMatcher struct {
	Prefix netip.Prefix
}

// Match implements IPMatcher.
func (m CIDRMatcher) Match(ip netip.Addr) bool {
	return m.Prefix.Contains(ip)
}
