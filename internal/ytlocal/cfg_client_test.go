package ytlocal

import (
	"testing"
)

func TestClient(t *testing.T) {
	base := newBaseServer()
	encode(t, "client", Client{
		AddressResolver: base.AddressResolver,
		Driver: Driver{
			MasterCache: MasterCache{
				EnableMasterCacheDiscovery: true,
				Addresses:                  base.ClusterConnection.PrimaryMaster.Addresses,
				CellID:                     base.ClusterConnection.PrimaryMaster.CellID,
			},
			TimestampProvider: base.TimestampProvider,
			PrimaryMaster:     base.ClusterConnection.PrimaryMaster,
			APIVersion:        4,
		},
	})
}
