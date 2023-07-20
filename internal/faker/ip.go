package faker

import (
	"encoding/binary"
	"net/netip"
	"sync/atomic"
)

// ipAllocator is sequential ip address allocator without garbage collection.
type ipAllocator struct {
	offset uint32
}

// Next allocates new ip address. It panics if the address space is exhausted.
// It is safe for concurrent use.
func (a *ipAllocator) Next() netip.Addr {
	data := binary.BigEndian.AppendUint32(nil, atomic.AddUint32(&a.offset, 1))
	ip, ok := netip.AddrFromSlice(data)
	if !ok {
		panic("ip address overflow")
	}
	return ip
}

func newIPAllocator(ip netip.Addr) *ipAllocator {
	addr := binary.BigEndian.Uint32(ip.AsSlice())
	return &ipAllocator{
		offset: addr,
	}
}
