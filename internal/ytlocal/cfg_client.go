package ytlocal

// AddressResolver config.
type AddressResolver struct {
	Retries    int  `yson:"retries"`
	EnableIPv6 bool `yson:"enable_ipv6"`
	EnableIPv4 bool `yson:"enable_ipv4"`
}

// Client config.
type Client struct {
	AddressResolver AddressResolver `yson:"address_resolver"`
	Driver          Driver          `yson:"driver"`
}

// Connection config.
type Connection struct {
	Addresses []string `yson:"addresses"`
	CellID    string   `yson:"cell_id,omitempty"`
}

// MasterCache config.
type MasterCache struct {
	EnableMasterCacheDiscovery bool     `yson:"enable_master_cache_discovery"`
	Addresses                  []string `yson:"addresses"`
	CellID                     string   `yson:"cell_id,omitempty"`
}

// Driver config.
type Driver struct {
	MasterCache       MasterCache `yson:"master_cache,omitempty"`
	TimestampProvider Connection  `yson:"timestamp_provider,omitempty"`
	PrimaryMaster     Connection  `yson:"primary_master,omitempty"`
	APIVersion        int         `yson:"api_version"`
}
