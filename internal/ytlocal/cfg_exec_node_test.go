package ytlocal

import "testing"

func TestExecNode(t *testing.T) {
	encode(t, "exec-node", ExecNode{
		BaseServer: newBaseServer(),
		ResourceLimits: ResourceLimits{
			TotalCPU:         0.0,
			TotalMemory:      8388608000,
			NodeDedicatedCPU: 0.0,
		},
		Flavors: []string{"exec"},
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
		BaseServer: newBaseServer(),
		ResourceLimits: ResourceLimits{
			TotalCPU:         0.0,
			TotalMemory:      8388608000,
			NodeDedicatedCPU: 0.0,
		},
		Flavors: []string{"exec"},
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
