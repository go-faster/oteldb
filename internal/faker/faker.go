// Package faker implement a fake telemetry generator.
package faker

// Config models a single cluster of multiple nodes, where services are
// deployed on each node.
//
// Services are interacting with each other, generating telemetry.
//
// Multiple clients are modeled, sending requests to the services.
type Config struct {
	// Number of nodes.
	Nodes int `json:"nodes" yaml:"nodes"`
	// Number of requests per second.
	RPS int `json:"rps" yaml:"rps"`
	// Services configuration.
	Services Services `json:"services" yaml:"services"`
}

type Services struct {
	API      ServiceAPI `json:"api" yaml:"api"`
	Backend  Backend    `json:"backend" yaml:"backend"`
	Frontend Frontend   `json:"frontend" yaml:"frontend"`
}

type ServiceAPI struct {
	Replicas int `json:"replicas" yaml:"replicas"`
}

type Backend struct {
	Replicas int `json:"replicas" yaml:"replicas"`
}

type Frontend struct {
	Replicas int `json:"replicas" yaml:"replicas"`
}
