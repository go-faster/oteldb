package ytlocal

type ResourceLimits struct {
	TotalCPU         float64 `yson:"total_cpu"`
	TotalMemory      int64   `yson:"total_memory"`
	NodeDedicatedCPU float64 `yson:"node_dedicated_cpu"`
}

type StoreLocation struct {
	Quota                  int64  `yson:"quota"`
	MediumName             string `yson:"medium_name"`
	LowWatermark           int64  `yson:"low_watermark"`
	DisableWritesWatermark int64  `yson:"disable_writes_watermark"`
	Path                   string `yson:"path"`
	HighWatermark          int64  `yson:"high_watermark"`
}

type DataNodeOptions struct {
	StoreLocations []StoreLocation `yson:"store_locations"`
}

type DataNode struct {
	TimestampProvider *Connection        `yson:"timestamp_provider,omitempty"`
	ClusterConnection *ClusterConnection `yson:"cluster_connection,omitempty"`
	ResourceLimits    ResourceLimits     `yson:"resource_limits"`
	Flavors           []string           `yson:"flavors"`
	AddressResolver   AddressResolver    `yson:"address_resolver"`
	Options           DataNodeOptions    `yson:"data_node"`
	RPCPort           int                `yson:"rpc_port"`
	MonitoringPort    int                `yson:"monitoring_port"`
}
