package ytlocal

// Strawberry config.
type Strawberry struct {
	Root          string `yson:"root"`
	Stage         string `yson:"stage"`
	RobotUsername string `yson:"robot_username"`
}

// CHYTController config.
type CHYTController struct {
	LocationProxies        []string   `yson:"location_proxies"`
	Strawberry             Strawberry `yson:"strawberry"`
	Controller             any        `yson:"controller"`
	HTTPAPIEndpoint        string     `yson:"http_api_endpoint,omitempty"`
	HTTPMonitoringEndpoint string     `yson:"http_monitoring_endpoint,omitempty"`
	DisableAPIAuth         bool       `yson:"disable_api_auth"`
}

// CHYTInitCluster config.
type CHYTInitCluster struct {
	Proxy          string `yson:"proxy"`
	StrawberryRoot string `yson:"strawberry_root"`
}
