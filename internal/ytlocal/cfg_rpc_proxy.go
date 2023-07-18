package ytlocal

// RPCProxy is rpc-proxy config.
type RPCProxy struct {
	BaseServer
	Role                      string                    `yson:"role"`
	CypressTokenAuthenticator CypressTokenAuthenticator `yson:"cypress_token_authenticator"`
}
