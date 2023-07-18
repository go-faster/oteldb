package ytlocal

// ControllerAgent config.
type ControllerAgent struct {
	BaseServer
	Options ControllerAgentOptions `yson:"controller_agent"`
}

// ControllerAgentOptions config.
type ControllerAgentOptions struct {
	UseColumnarStatisticsDefault bool `yson:"use_columnar_statistics_default"`
	EnableTMPFS                  bool `yson:"enable_tmpfs"`
}
