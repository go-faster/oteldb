package ytlocal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPortAllocator_Allocate(t *testing.T) {
	a := &PortAllocator{}
	seen := make(map[int]struct{})
	for i := 0; i < 42; i++ {
		port, err := a.Allocate()
		require.NoError(t, err, "allocate")
		if _, ok := seen[port]; ok {
			t.Fatalf("port %d already seen", port)
		}
		seen[port] = struct{}{}
		require.NotZero(t, port, "port should not be zero")
	}
}
