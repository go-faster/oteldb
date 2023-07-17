package ytlocal

import "testing"

func TestDataNode(t *testing.T) {
	encode(t, "data-node", DataNode{
		MonitoringPort: 10014,
		TimestampProvider: &Connection{
			Addresses: []string{
				"1.master.yt.go-faster.org:9010",
				"2.master.yt.go-faster.org:9010",
				"3.master.yt.go-faster.org:9010",
			},
		},
		ClusterConnection: &ClusterConnection{
			ClusterName: "ytlocal",
			DiscoveryConnection: &Connection{
				Addresses: []string{
					"1.master.yt.go-faster.org:9020",
					"2.master.yt.go-faster.org:9020",
					"3.master.yt.go-faster.org:9020",
				},
			},
			PrimaryMaster: &Connection{
				Addresses: []string{
					"1.master.yt.go-faster.org:9010",
					"2.master.yt.go-faster.org:9010",
					"3.master.yt.go-faster.org:9010",
				},
				CellID: "ffcef5128-9be15fe9-10242-ffffffcb",
			},
		},
		ResourceLimits: ResourceLimits{
			TotalCPU:         0.0,
			TotalMemory:      8388608000,
			NodeDedicatedCPU: 0.0,
		},
		Flavors: []string{"data"},
		AddressResolver: AddressResolver{
			Retries:    1000,
			EnableIPv6: false,
			EnableIPv4: true,
		},
		RPCPort: 9015,
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
