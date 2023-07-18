package ytlocal

import (
	"testing"
)

func TestClient(t *testing.T) {
	encode(t, "client", Client{
		AddressResolver: AddressResolver{
			Retries:    1000,
			EnableIPv6: false,
			EnableIPv4: true,
		},
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
	})
}
