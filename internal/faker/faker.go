// Package faker implement a fake telemetry generator.
package faker

import (
	"math/rand"

	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

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
	// Random number generator.
	Rand *rand.Rand
	// Factory for TracerProvider that can create providers from new
	TracerProviderFactory TracerProviderFactory
}

// TracerProviderFactory is a factory for TracerProvider.
type TracerProviderFactory interface {
	New(...tracesdk.TracerProviderOption) *tracesdk.TracerProvider
}

// Services wraps all services configuration, describing topology of the
// cluster and load.
type Services struct {
	API      API      `json:"api" yaml:"api"`
	Backend  Backend  `json:"backend" yaml:"backend"`
	Frontend Frontend `json:"frontend" yaml:"frontend"`
	DB       DB       `json:"db" yaml:"db"`
	Cache    Cache    `json:"cache" yaml:"cache"`
}

// API represents API service configuration.
//
// API service acts as an entrypoint and handles requests from clients, proxying
// them to [Backend].
type API struct {
	Replicas int `json:"replicas" yaml:"replicas"`
}

// Backend handles requests from [API] service.
//
// Backend service is used to process user requests from [API].
// Handles business logic and uses database and cache services.
type Backend struct {
	Replicas int `json:"replicas" yaml:"replicas"`
}

// DB represents database service configuration.
type DB struct {
	Replicas int `json:"replicas" yaml:"replicas"`
}

// Cache represents cache service configuration.
//
// Cache service is used by [Backend]for short term data storage with
// fast access.
//
// Example: Redis.
type Cache struct {
	Replicas int `json:"replicas" yaml:"replicas"`
}

// Frontend represents user web client configuration.
//
// Frontend service sends requests to [API] on behalf of the user.
type Frontend struct {
	Replicas int `json:"replicas" yaml:"replicas"`
}
