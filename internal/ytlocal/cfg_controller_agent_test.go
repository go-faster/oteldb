package ytlocal

import "testing"

func TestControllerAgent(t *testing.T) {
	encode(t, "controller-agent", ControllerAgent{
		BaseServer: newBaseServer(),
		Options: ControllerAgentOptions{
			UseColumnarStatisticsDefault: true,
			EnableTMPFS:                  true,
		},
	})
}
