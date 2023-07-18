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
	BaseServer
	Port        int         `yson:"port"`
	Role        string      `yson:"role"`
	ThreadCount int         `yson:"thread_count"`
	Driver      Driver      `yson:"driver"`
	Auth        HTTPAuth    `yson:"auth"`
	Coordinator Coordinator `yson:"coordinator,omitempty"`
}
