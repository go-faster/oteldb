package otelreceiver

import (
	"context"

	"go.opentelemetry.io/collector/confmap"
)

var _ confmap.Provider = (*mapProvider)(nil)

// mapProvider is a confmap.Provider that returns a single confmap.Retrieved instance with a fixed map.
type mapProvider struct {
	raw map[string]any
}

func (m *mapProvider) Retrieve(context.Context, string, confmap.WatcherFunc) (*confmap.Retrieved, error) {
	return confmap.NewRetrieved(m.raw, []confmap.RetrievedOption{}...)
}

func (m *mapProvider) Scheme() string { return "mock" }

func (m *mapProvider) Shutdown(context.Context) error { return nil }
