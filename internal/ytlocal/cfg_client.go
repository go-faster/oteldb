package ytlocal

// Client config.
type Client struct {
	AddressResolver AddressResolver `yson:"address_resolver"`
	Driver          Driver          `yson:"driver"`
}

// MasterCache config.
type MasterCache struct {
	EnableMasterCacheDiscovery bool     `yson:"enable_master_cache_discovery"`
	Addresses                  []string `yson:"addresses"`
	CellID                     string   `yson:"cell_id,omitempty"`
}

// Driver config.
type Driver struct {
	MasterCache       MasterCache `yson:"master_cache"`
	TimestampProvider Connection  `yson:"timestamp_provider"`
	PrimaryMaster     Connection  `yson:"primary_master"`
	APIVersion        int         `yson:"api_version"`
}
