package ytlocal

import "testing"

func TestRPCProxy(t *testing.T) {
	encode(t, "rpc-proxy", RPCProxy{
		BaseServer: newBaseServer(),
		Role:       "default",
		CypressTokenAuthenticator: CypressTokenAuthenticator{
			Secure: true,
		},
	})
}
