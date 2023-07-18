package ytlocal

// QueryTracker config.
type QueryTracker struct {
	TimestampProvider          Connection        `yson:"timestamp_provider,omitempty"`
	User                       string            `yson:"user"`
	ClusterConnection          ClusterConnection `yson:"cluster_connection,omitempty"`
	AddressResolver            AddressResolver   `yson:"address_resolver"`
	RPCPort                    int               `yson:"rpc_port"`
	MonitoringPort             int               `yson:"monitoring_port"`
	CreateStateTablesOnStartup bool              `yson:"create_state_tables_on_startup"`
}
