package ytlocal

import "testing"

func TestMaster(t *testing.T) {
	encode(t, "master", Master{
		RPCPort:        9020,
		MonitoringPort: 10020,
		AddressResolver: AddressResolver{
			Retries:    1000,
			EnableIPv6: false,
			EnableIPv4: true,
		},
		PrimaryMaster: Connection{
			Addresses: []string{
				"1.master.yt.go-faster.org:9010",
				"2.master.yt.go-faster.org:9010",
				"3.master.yt.go-faster.org:9010",
			},
			CellID: "ffcef5128-9be15fe9-10242-ffffffcb",
		},
		TimestampProvider: Connection{
			Addresses: []string{
				"1.master.yt.go-faster.org:9010",
				"2.master.yt.go-faster.org:9010",
				"3.master.yt.go-faster.org:9010",
			},
		},
		CypressManager: CypressManager{
			DefaultJournalReadQuorum:     3,
			DefaultFileReplicationFactor: 5,
			DefaultJournalWriteQuorum:    3,
		},
		HydraManager: HydraManager{
			MaxChangelogCountToKeep: 2,
			MaxSnapshotCountToKeep:  2,
		},
		Changelogs: MasterChangelogs{
			Path: "/var/lib/yt/data/master/changelogs",
		},
		Snapshots: MasterSnapshots{
			Path: "/var/lib/yt/data/master/snapshots",
		},
		SecondaryMasters: []Connection{},
		ClusterConnection: ClusterConnection{
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
	})
}
