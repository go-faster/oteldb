package ytlocal

import "testing"

func TestTabletNode(t *testing.T) {
	encode(t, "tablet-node", TabletNode{
		BaseServer: newBaseServer(),
		Flavors: []string{
			"tablet",
		},
		ResourceLimits: ResourceLimits{
			TotalCPU:         0.0,
			TotalMemory:      8388608000,
			NodeDedicatedCPU: 0.0,
		},
	})
}
