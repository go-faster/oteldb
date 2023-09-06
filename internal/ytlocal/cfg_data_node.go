package ytlocal

// ResourceLimits config.
type ResourceLimits struct {
	TotalCPU         float64 `yson:"total_cpu,omitempty"`
	TotalMemory      int64   `yson:"total_memory,omitempty"`
	NodeDedicatedCPU float64 `yson:"node_dedicated_cpu"`
	Memory           int64   `yson:"memory,omitempty"`
}

// StoreLocation config.
type StoreLocation struct {
	Quota                  int64  `yson:"quota,omitempty"`
	MediumName             string `yson:"medium_name,omitempty"`
	LowWatermark           int64  `yson:"low_watermark,omitempty"`
	DisableWritesWatermark int64  `yson:"disable_writes_watermark,omitempty"`
	Path                   string `yson:"path,omitempty"`
	HighWatermark          int64  `yson:"high_watermark,omitempty"`
}

// DataNodeOptions config.
type DataNodeOptions struct {
	StoreLocations []StoreLocation `yson:"store_locations"`
	CacheLocations []DiskLocation  `yson:"cache_locations"`
	BlockCache     BlockCache      `yson:"block_cache"`
	BlocksExtCache Cache           `yson:"blocks_ext_cache"`
	ChunkMetaCache Cache           `yson:"chunk_meta_cache"`
	BlockMetaCache Cache           `yson:"block_meta_cache"`
}

// DiskLocation config.
type DiskLocation struct {
	Path string `yson:"path"`
}

// Cache config.
type Cache struct {
	Capacity int64 `yson:"capacity"`
}

// BlockCache config
type BlockCache struct {
	Compressed   Cache `yson:"compressed_data"`
	Uncompressed Cache `yson:"uncompressed_data"`
}

// Node config.
type Node struct {
	BaseServer
	Flavors        []string        `yson:"flavors,omitempty"`
	ResourceLimits ResourceLimits  `yson:"resource_limits"`
	Options        DataNodeOptions `yson:"data_node"`
	Addresses      [][]string      `yson:"addresses,omitempty"`
}
