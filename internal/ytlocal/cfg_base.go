package ytlocal

// AddressResolver config.
type AddressResolver struct {
	Retries       int    `yson:"retries"`
	EnableIPv6    bool   `yson:"enable_ipv6"`
	EnableIPv4    bool   `yson:"enable_ipv4"`
	LocalhostFQDN string `yson:"localhost_fqdn,omitempty"`
}

// Connection config.
type Connection struct {
	Addresses []string `yson:"addresses"`
	CellID    string   `yson:"cell_id,omitempty"`
}

// ClusterConnection config.
type ClusterConnection struct {
	ClusterName         string     `yson:"cluster_name"`
	DiscoveryConnection Connection `yson:"discovery_connection"`
	PrimaryMaster       Connection `yson:"primary_master"`
}

// BaseServer wraps common server configiguratio parts.
type BaseServer struct {
	RPCPort           int               `yson:"rpc_port"`
	MonitoringPort    int               `yson:"monitoring_port"`
	Logging           Logging           `yson:"logging"`
	AddressResolver   AddressResolver   `yson:"address_resolver"`
	TimestampProvider Connection        `yson:"timestamp_provider"`
	ClusterConnection ClusterConnection `yson:"cluster_connection"`
}
