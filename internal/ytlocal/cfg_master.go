package ytlocal

// IOEngine is io_engine config.
type IOEngine struct {
	EnableSync bool `yson:"enable_sync"`
}

// MasterChangelogs config.
type MasterChangelogs struct {
	Path        string   `yson:"path"`
	FlushPeriod int      `yson:"flush_period,omitempty"`
	EnableSync  bool     `yson:"enable_sync"`
	IOEngine    IOEngine `yson:"io_engine"`
}

// ObjectService is master object service config.
type ObjectService struct {
	EnableLocalReadExecutor bool `yson:"enable_local_read_executor"`
	EnableLocalReadBusyWait bool `yson:"enable_local_read_busy_wait"`
}

// MasterSnapshots config.
type MasterSnapshots struct {
	Path string `yson:"path"`
}

// ResponseKeeper is hydra manager response keeper config.
type ResponseKeeper struct {
	EnableWarmup   bool `yson:"enable_warmup"`
	ExpirationTime int  `yson:"expiration_time"`
	WarmupTime     int  `yson:"warmup_time"`
}

// HydraManager config.
type HydraManager struct {
	SnapshotBackgroundThreadCount int            `yson:"snapshot_background_thread_count,omitempty"`
	LeaderSyncDelay               int            `yson:"leader_sync_delay,omitempty"`
	MinimizeCommitLatency         bool           `yson:"minimize_commit_latency"`
	LeaderLeaseCheckPeriod        int            `yson:"leader_lease_check_period,omitempty"`
	LeaderLeaseTimeout            int            `yson:"leader_lease_timeout,omitempty"`
	DisableLeaderLeaseGraceDelay  bool           `yson:"disable_leader_lease_grace_delay,omitempty"`
	InvariantsCheckProbability    float64        `yson:"invariants_check_probability"`
	MaxChangelogCountToKeep       int            `yson:"max_changelog_count_to_keep,omitempty"`
	MaxSnapshotCountToKeep        int            `yson:"max_snapshot_count_to_keep,omitempty"`
	MaxChangelogDataSize          int            `yson:"max_changelog_data_size,omitempty"`
	ResponseKeeper                ResponseKeeper `yson:"response_keeper"`
}

// CypressManager config.
type CypressManager struct {
	DefaultTableReplicationFactor   int `yson:"default_table_replication_factor,omitempty"`
	DefaultFileReplicationFactor    int `yson:"default_file_replication_factor,omitempty"`
	DefaultJournalReplicationFactor int `yson:"default_journal_replication_factor,omitempty"`
	DefaultJournalReadQuorum        int `yson:"default_journal_read_quorum,omitempty"`
	DefaultJournalWriteQuorum       int `yson:"default_journal_write_quorum,omitempty"`
}

type TimestampManager struct {
	CommitAdvance      int `yson:"commit_advance"`
	RequestBackoffTime int `yson:"request_backoff_time"`
	CalibrationPeriod  int `yson:"calibration_period"`
}

type HiveManager struct {
	PingPeriod     int `yson:"ping_period"`
	IdlePostPeriod int `yson:"idle_post_period"`
}

type YPServiceDiscovery struct {
	Enable bool `yson:"enable"`
}

type RPCDispatcher struct {
	CompressionPoolSize int `yson:"compression_pool_size"`
	HeavyPoolSize       int `yson:"heavy_pool_size"`
}

type ChunkClientDispatcher struct {
	ChunkReaderPoolSize int `yson:"chunk_reader_pool_size"`
}

type TCPDispatcher struct {
	ThreadPoolSize int `yson:"thread_pool_size"`
}

type SolomonExporter struct {
	GridStep int `yson:"grid_step"`
}

type CypressAnnotations struct {
	YTEnvIndex int `yson:"yt_env_index"`
}

type ChunkManger struct {
	AllowMultipleErasurePartsPerNode bool `yson:"allow_multiple_erasure_parts_per_node"`
}

// Master config.
type Master struct {
	BaseServer
	PrimaryMaster                    Connection            `yson:"primary_master"`
	UseNewHydra                      bool                  `yson:"use_new_hydra"`
	EnableProvisionLock              bool                  `yson:"enable_provision_lock"`
	Changelogs                       MasterChangelogs      `yson:"changelogs"`
	ObjectService                    ObjectService         `yson:"object_service"`
	Snapshots                        MasterSnapshots       `yson:"snapshots"`
	HydraManager                     HydraManager          `yson:"hydra_manager"`
	YPServiceDiscovery               YPServiceDiscovery    `yson:"yp_service_discovery"`
	RPCDispatcher                    RPCDispatcher         `yson:"rpc_dispatcher"`
	ChunkClientDispatcher            ChunkClientDispatcher `yson:"chunk_client_dispatcher"`
	TCPDispatcher                    TCPDispatcher         `yson:"tcp_dispatcher"`
	SolomonExporter                  SolomonExporter       `yson:"solomon_exporter"`
	CypressAnnotations               CypressAnnotations    `yson:"cypress_annotations"`
	EnableRefCountedTrackerProfiling bool                  `yson:"enable_ref_counted_tracker_profiling"`
	EnableResourceTracker            bool                  `yson:"enable_resource_tracker"`
	EnableTimestampManager           bool                  `yson:"enable_timestamp_manager"`
	TimestampManager                 TimestampManager      `yson:"timestamp_manager"`
	HiveManager                      HiveManager           `yson:"hive_manager"`
	CypressManager                   CypressManager        `yson:"cypress_manager"`
	SecondaryMasters                 []Connection          `yson:"secondary_masters"`
	ChunkManger                      ChunkManger           `yson:"chunk_manager"`
}
