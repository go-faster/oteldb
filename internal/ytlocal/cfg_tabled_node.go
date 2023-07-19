package ytlocal

// TabletNode is tabled-node config.
type TabletNode struct {
	BaseServer
	Flavors        []string       `yson:"flavors"`
	ResourceLimits ResourceLimits `yson:"resource_limits"`
}
