package ytlocal

// ResourceLimits config.
type ResourceLimits struct {
	TotalCPU         float64 `yson:"total_cpu"`
	TotalMemory      int64   `yson:"total_memory"`
	NodeDedicatedCPU float64 `yson:"node_dedicated_cpu"`
}

// StoreLocation config.
type StoreLocation struct {
	Quota                  int64  `yson:"quota"`
	MediumName             string `yson:"medium_name"`
	LowWatermark           int64  `yson:"low_watermark"`
	DisableWritesWatermark int64  `yson:"disable_writes_watermark"`
	Path                   string `yson:"path"`
	HighWatermark          int64  `yson:"high_watermark"`
}

// DataNodeOptions config.
type DataNodeOptions struct {
	StoreLocations []StoreLocation `yson:"store_locations"`
}

// DataNode config.
type DataNode struct {
	BaseServer
	Flavors        []string        `yson:"flavors"`
	ResourceLimits ResourceLimits  `yson:"resource_limits"`
	Options        DataNodeOptions `yson:"data_node"`
}
