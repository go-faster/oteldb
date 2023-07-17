package ytlocal

import "testing"

func TestExecNode(t *testing.T) {
	encode(t, "exec-node", ExecNode{
		RPCPort:        9020,
		MonitoringPort: 10020,
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
			},
		},
		ResourceLimits: ResourceLimits{
			TotalCPU:         0.0,
			TotalMemory:      8388608000,
			NodeDedicatedCPU: 0.0,
		},
		Flavors: []string{"exec"},
		AddressResolver: AddressResolver{
			Retries:    1000,
			EnableIPv6: false,
			EnableIPv4: true,
		},
		TabletNode: TabletNodeConnection{
			VersionedChunkMetaCache: VersionedChunkMetaCache{
				Capacity: 67108864,
			},
		},
		ExecAgent: ExecAgent{
			SlotManager: SlotManager{
				Locations: []SlotLocation{
					{
						Path:               "/store/slot/node",
						DiskQuota:          5368709120,
						DiskUsageWatermark: 161061273,
					},
				},
				JobEnvironment: JobEnvironment{
					StartUID: 19500,
					Type:     JobEnvironmentTypePorto,
				},
			},
		},
	})
	encode(t, "exec-node-simple", ExecNode{
		RPCPort:        9020,
		MonitoringPort: 10020,
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
			},
		},
		ResourceLimits: ResourceLimits{
			TotalCPU:         0.0,
			TotalMemory:      8388608000,
			NodeDedicatedCPU: 0.0,
		},
		Flavors: []string{"exec"},
		AddressResolver: AddressResolver{
			Retries:    1000,
			EnableIPv6: false,
			EnableIPv4: true,
		},
		TabletNode: TabletNodeConnection{
			VersionedChunkMetaCache: VersionedChunkMetaCache{
				Capacity: 67108864,
			},
		},
		ExecAgent: ExecAgent{
			SlotManager: SlotManager{
				Locations: []SlotLocation{
					{
						Path:               "/store/slot/node",
						DiskQuota:          5368709120,
						DiskUsageWatermark: 161061273,
					},
				},
				JobEnvironment: JobEnvironment{
					StartUID: 19500,
					Type:     JobEnvironmentTypeSimple,
				},
			},
		},
	})
}
