package ytlocal

import "testing"

func TestHTTPProxy(t *testing.T) {
	encode(t, "http-proxy", HTTPProxy{
		Port:           80,
		RPCPort:        9020,
		MonitoringPort: 10020,
		Role:           "default",
		Driver: Driver{
			MasterCache: MasterCache{
				EnableMasterCacheDiscovery: true,
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
			PrimaryMaster: Connection{
				Addresses: []string{
					"1.master.yt.go-faster.org:9010",
					"2.master.yt.go-faster.org:9010",
					"3.master.yt.go-faster.org:9010",
				},
				CellID: "ffcef5128-9be15fe9-10242-ffffffcb",
			},
			APIVersion: 4,
		},
		AddressResolver: AddressResolver{
			Retries:    1000,
			EnableIPv6: false,
			EnableIPv4: true,
		},
		Auth: HTTPAuth{
			RequireAuthentication: true,
			CypressTokenAuthenticator: CypressTokenAuthenticator{
				Secure: true,
			},
			CypressCookieManager: CypressCookieManager{},
		},
		Coordinator: Coordinator{
			DefaultRoleFilter: "default",
			Enable:            true,
		},
		ClusterConnection: ClusterConnection{
			ClusterName: "ytlocal",
			DiscoveryConnection: Connection{
				Addresses: []string{
					"1.master.yt.go-faster.org:9020",
					"2.master.yt.go-faster.org:9020",
					"3.master.yt.go-faster.org:9020",
				},
			},
			PrimaryMaster: Connection{
				Addresses: []string{
					"1.master.yt.go-faster.org:9010",
					"2.master.yt.go-faster.org:9010",
					"3.master.yt.go-faster.org:9010",
				},
			},
		},
	})
}
