package ytlocal

// VersionedChunkMetaCache config.
type VersionedChunkMetaCache struct {
	Capacity int `yson:"capacity"`
}

// TabletNodeConnection config.
type TabletNodeConnection struct {
	VersionedChunkMetaCache VersionedChunkMetaCache `yson:"versioned_chunk_meta_cache"`
}

// SlotLocation config.
type SlotLocation struct {
	Path               string `yson:"path"`
	DiskQuota          int64  `yson:"disk_quota"`
	DiskUsageWatermark int64  `yson:"disk_usage_watermark"`
}

// SlotManager config.
type SlotManager struct {
	JobEnvironment JobEnvironment `yson:"job_environment"`
	Locations      []SlotLocation `yson:"locations"`
}

// JobEnvironmentType is type for environment (runtime).
type JobEnvironmentType string

// Possible types.
const (
	JobEnvironmentTypeSimple JobEnvironmentType = "simple"
	JobEnvironmentTypePorto  JobEnvironmentType = "porto"
)

// JobEnvironment config.
type JobEnvironment struct {
	StartUID int64              `yson:"start_uid"`
	Type     JobEnvironmentType `yson:"type"`
}

// JobControllerResourceLimits config.
type JobControllerResourceLimits struct {
	UserSlots int `yson:"user_slots"`
}

// JobController config.
type JobController struct {
	ResourceLimits JobControllerResourceLimits `yson:"resource_limits"`
}

// ExecAgent config.
type ExecAgent struct {
	SlotManager SlotManager `yson:"slot_manager"`
}

// MasterCacheService config.
type MasterCacheService struct {
	Capacity int64 `yson:"capaticy"`
}

// ExecNode config.
type ExecNode struct {
	BaseServer
	Flavors        []string             `yson:"flavors"`
	ResourceLimits ResourceLimits       `yson:"resource_limits"`
	TabletNode     TabletNodeConnection `yson:"tablet_node"`
	ExecAgent      ExecAgent            `yson:"exec_agent"`
}
