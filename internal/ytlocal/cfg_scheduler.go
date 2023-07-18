package ytlocal

// Scheduler config.
type Scheduler struct {
	AddressResolver   *AddressResolver   `yson:"address_resolver"`
	RPCPort           int                `yson:"rpc_port"`
	MonitoringPort    int                `yson:"monitoring_port"`
	TimestampProvider *Connection        `yson:"timestamp_provider,omitempty"`
	ClusterConnection *ClusterConnection `yson:"cluster_connection,omitempty"`
}
