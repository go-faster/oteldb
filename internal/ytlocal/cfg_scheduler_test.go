package ytlocal

import "testing"

func TestScheduler(t *testing.T) {
	encode(t, "scheduler", Scheduler{
		RPCPort:        9020,
		MonitoringPort: 10020,
		AddressResolver: &AddressResolver{
			Retries:    1000,
			EnableIPv6: false,
			EnableIPv4: true,
		},
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
			},
		},
	})
}
