package ytlocal

import "testing"

func TestControllerAgent(t *testing.T) {
	encode(t, "controller-agent", ControllerAgent{
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
		AddressResolver: AddressResolver{
			Retries:    1000,
			EnableIPv6: false,
			EnableIPv4: true,
		},
		RPCPort: 9015,
		Options: ControllerAgentOptions{
			UseColumnarStatisticsDefault: true,
			EnableTMPFS:                  true,
		},
	})
}
