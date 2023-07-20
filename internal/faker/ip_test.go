package faker

import (
	"net/netip"
	"testing"
)

func TestIPAllocator_Next(t *testing.T) {
	allocator := newIPAllocator(netip.MustParseAddr("10.0.5.0"))
	seen := make(map[netip.Addr]struct{})
	for i := 0; i < 1600; i++ {
		ip := allocator.Next()
		if _, ok := seen[ip]; ok {
			t.Fatalf("duplicate ip address: %s", ip)
		}
		seen[ip] = struct{}{}
	}
	t.Logf("%s", allocator.Next())
}
