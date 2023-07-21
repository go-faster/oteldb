package ytlocal

import (
	"net"
	"strconv"
	"sync"

	"github.com/go-faster/errors"
)

// PortAllocator is a helper to allocate random ports.
type PortAllocator struct {
	Host string
	Net  string

	mux  sync.Mutex
	seen map[int]struct{}
}

func (p *PortAllocator) allocate() (int, error) {
	network := p.Net
	if network == "" {
		network = "tcp"
	}
	host := p.Host
	if host == "" {
		host = "localhost"
	}
	l, err := net.Listen(network, net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, errors.Wrap(err, "listen")
	}
	if err := l.Close(); err != nil {
		return 0, errors.Wrap(err, "close listener")
	}
	switch a := l.Addr().(type) {
	case *net.TCPAddr:
		return a.Port, nil
	case *net.UDPAddr:
		return a.Port, nil
	default:
		_, port, err := net.SplitHostPort(l.Addr().String())
		if err != nil {
			return 0, errors.Wrap(err, "split host port")
		}
		return strconv.Atoi(port)
	}
}

// Allocate a new random port.
//
// Tries to allocate a new port 10 times before returning an error.
// Safe to call concurrently.
func (p *PortAllocator) Allocate() (int, error) {
	p.mux.Lock()
	defer p.mux.Unlock()

	if p.seen == nil {
		p.seen = make(map[int]struct{})
	}
	attempts := 10
	for i := 0; i < attempts; i++ {
		port, err := p.allocate()
		if err != nil {
			return 0, errors.Wrap(err, "allocate")
		}
		if _, ok := p.seen[port]; ok {
			continue
		}
		p.seen[port] = struct{}{}
		return port, nil
	}
	return 0, errors.New("failed to allocate port")
}
