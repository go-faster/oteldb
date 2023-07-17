package ytlocal

type ClusterConnection struct {
	ClusterName         string      `yson:"cluster_name"`
	DiscoveryConnection *Connection `yson:"discovery_connection,omitempty"`
	PrimaryMaster       *Connection `yson:"primary_master,omitempty"`
}

type ControllerAgent struct {
	MonitoringPort    int                    `yson:"monitoring_port"`
	TimestampProvider *Connection            `yson:"timestamp_provider,omitempty"`
	ClusterConnection *ClusterConnection     `yson:"cluster_connection,omitempty"`
	AddressResolver   AddressResolver        `yson:"address_resolver"`
	RPCPort           int                    `yson:"rpc_port"`
	Options           ControllerAgentOptions `yson:"controller_agent"`
}

type ControllerAgentOptions struct {
	UseColumnarStatisticsDefault bool `yson:"use_columnar_statistics_default"`
	EnableTMPFS                  bool `yson:"enable_tmpfs"`
}
