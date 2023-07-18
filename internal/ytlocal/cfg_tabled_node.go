package ytlocal

// TabletNode is tabled-node config.
type TabletNode struct {
	RPCPort           int                `yson:"rpc_port"`
	MonitoringPort    int                `yson:"monitoring_port"`
	TimestampProvider *Connection        `yson:"timestamp_provider,omitempty"`
	ClusterConnection *ClusterConnection `yson:"cluster_connection,omitempty"`
	AddressResolver   *AddressResolver   `yson:"address_resolver"`
	Flavors           []string           `yson:"flavors"`
	ResourceLimits    *ResourceLimits    `yson:"resource_limits"`
}
