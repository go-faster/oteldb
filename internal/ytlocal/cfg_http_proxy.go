package ytlocal

// Coordinator config.
type Coordinator struct {
	DefaultRoleFilter string `yson:"default_role_filter"`
	Enable            bool   `yson:"enable"`
}

// CypressTokenAuthenticator config.
type CypressTokenAuthenticator struct {
	Secure bool `yson:"secure"`
}

// CypressCookieManager config.
type CypressCookieManager struct{}

// HTTPAuth configures http-proxy auth.
type HTTPAuth struct {
	RequireAuthentication     bool                      `yson:"require_authentication"`
	CypressTokenAuthenticator CypressTokenAuthenticator `yson:"cypress_token_authenticator"`
	CypressCookieManager      CypressCookieManager      `yson:"cypress_cookie_manager"`
}

// HTTPProxy is http-proxy component config.
type HTTPProxy struct {
	ClusterConnection ClusterConnection `yson:"cluster_connection,omitempty"`
	AddressResolver   AddressResolver   `yson:"address_resolver"`
	Role              string            `yson:"role"`
	Driver            Driver            `yson:"driver"`
	Port              int               `yson:"port"`
	RPCPort           int               `yson:"rpc_port"`
	ThreadCount       int               `yson:"thread_count"`
	Auth              HTTPAuth          `yson:"auth"`
	Coordinator       Coordinator       `yson:"coordinator,omitempty"`
	MonitoringPort    int               `yson:"monitoring_port"`
}
