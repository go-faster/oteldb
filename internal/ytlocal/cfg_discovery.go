package ytlocal

// DiscoveryServer config.
type DiscoveryServer struct {
	ServerAddresses []string `yson:"server_addresses"`
}

// Discovery config.
type Discovery struct {
	RPCPort         int             `yson:"rpc_port"`
	MonitoringPort  int             `yson:"monitoring_port"`
	DiscoveryServer DiscoveryServer `yson:"discovery_server"`
}
