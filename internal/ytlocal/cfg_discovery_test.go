package ytlocal

import "testing"

func TestDiscovery(t *testing.T) {
	encode(t, "discovery", Discovery{
		RPCPort:        9020,
		MonitoringPort: 10020,
		DiscoveryServer: DiscoveryServer{
			ServerAddresses: []string{
				"1.master.yt.go-faster.org:9020",
				"2.master.yt.go-faster.org:9020",
				"3.master.yt.go-faster.org:9020",
				"4.master.yt.go-faster.org:9020",
				"5.master.yt.go-faster.org:9020",
			},
		},
	})
}
