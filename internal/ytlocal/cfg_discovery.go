package ytlocal

type DiscoveryServer struct {
	ServerAddresses []string `yson:"server_addresses"`
}

type Discovery struct {
	RPCPort         int             `yson:"rpc_port"`
	MonitoringPort  int             `yson:"monitoring_port"`
	DiscoveryServer DiscoveryServer `yson:"discovery_server"`
}
