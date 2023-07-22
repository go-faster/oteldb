package ytlocal

import "testing"

func TestDataNode(t *testing.T) {
	encode(t, "data-node", Node{
		BaseServer: newBaseServer(),
		ResourceLimits: ResourceLimits{
			TotalCPU:         0.0,
			TotalMemory:      8388608000,
			NodeDedicatedCPU: 0.0,
		},
		Flavors: []string{"data"},
		Options: DataNodeOptions{
			StoreLocations: []StoreLocation{
				{
					Quota:                  32212254720,
					HighWatermark:          30064771072,
					MediumName:             "nvme",
					Path:                   "/dev/nvme0n1",
					LowWatermark:           31138512896,
					DisableWritesWatermark: 2147483648,
				},
				{
					Quota:                  10737418240,
					HighWatermark:          8589934592,
					MediumName:             "hdd",
					Path:                   "/dev/nvme0n1",
					LowWatermark:           9663676416,
					DisableWritesWatermark: 7516192768,
				},
			},
		},
	})
}
