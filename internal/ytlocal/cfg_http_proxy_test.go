package ytlocal

import "testing"

func TestHTTPProxy(t *testing.T) {
	encode(t, "http-proxy", HTTPProxy{
		Port:       80,
		BaseServer: newBaseServer(),
		Role:       "default",
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
	})
}
