package ytlocal

type VersionedChunkMetaCache struct {
	Capacity int `yson:"capacity"`
}

type TabletNodeConnection struct {
	VersionedChunkMetaCache VersionedChunkMetaCache `yson:"versioned_chunk_meta_cache"`
}

type SlotLocation struct {
	Path               string `yson:"path"`
	DiskQuota          int64  `yson:"disk_quota"`
	DiskUsageWatermark int64  `yson:"disk_usage_watermark"`
}

type SlotManager struct {
	JobEnvironment JobEnvironment `yson:"job_environment"`
	Locations      []SlotLocation `yson:"locations"`
}

type JobEnvironment struct {
	StartUID int64  `yson:"start_uid"`
	Type     string `yson:"type"`
}

type JobControllerResourceLimits struct {
	UserSlots int `yson:"user_slots"`
}

type JobController struct {
	ResourceLimits JobControllerResourceLimits `yson:"resource_limits"`
}

type ExecAgent struct {
	SlotManager SlotManager `yson:"slot_manager"`
}

type MasterCacheService struct {
	Capacity int64 `yson:"capaticy"`
}

type ExecNode struct {
	ClusterConnection *ClusterConnection   `yson:"cluster_connection,omitempty"`
	AddressResolver   AddressResolver      `yson:"address_resolver"`
	RPCPort           int                  `yson:"rpc_port"`
	MonitoringPort    int                  `yson:"monitoring_port"`
	TabletNode        TabletNodeConnection `yson:"tablet_node"`
	ExecAgent         ExecAgent            `yson:"exec_agent"`
	Flavors           []string             `yson:"flavors"`
	TimestampProvider *Connection          `yson:"timestamp_provider,omitempty"`
	ResourceLimits    ResourceLimits       `yson:"resource_limits"`
}
