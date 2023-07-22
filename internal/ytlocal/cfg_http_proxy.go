package ytlocal

// Coordinator config.
type Coordinator struct {
	DefaultRoleFilter string `yson:"default_role_filter,omitempty"`
	Enable            bool   `yson:"enable"`
	Announce          bool   `yson:"announce,omitempty"`
	ShowPorts         bool   `yson:"show_ports,omitempty"`
	PublicFQDN        string `yson:"public_fqdn,omitempty"`
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
	BaseServer
	Port        int         `yson:"port"`
	Role        string      `yson:"role,omitempty"`
	ThreadCount int         `yson:"thread_count"`
	Driver      Driver      `yson:"driver"`
	Auth        HTTPAuth    `yson:"auth"`
	Coordinator Coordinator `yson:"coordinator,omitempty"`
}
