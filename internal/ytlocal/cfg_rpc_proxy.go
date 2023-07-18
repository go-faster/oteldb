package ytlocal

type RPCProxy struct {
	AddressResolver           *AddressResolver           `yson:"address_resolver"`
	Role                      string                     `yson:"role"`
	RPCPort                   int                        `yson:"rpc_port"`
	MonitoringPort            int                        `yson:"monitoring_port"`
	TimestampProvider         *Connection                `yson:"timestamp_provider,omitempty"`
	ClusterConnection         *ClusterConnection         `yson:"cluster_connection,omitempty"`
	CypressTokenAuthenticator *CypressTokenAuthenticator `yson:"cypress_token_authenticator"`
}
