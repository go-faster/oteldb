package otelreceiver

import (
	"context"

	"go.opentelemetry.io/collector/confmap"
)

var _ confmap.Provider = (*mapProvider)(nil)

// mapProvider is a confmap.Provider that returns a single confmap.Retrieved instance with a fixed map.
type mapProvider struct {
	scheme string
	raw    map[string]any
}

// NewMapProvider creates new [mapProvider].
func NewMapProvider(scheme string, raw map[string]any) confmap.Provider {
	return &mapProvider{
		scheme: scheme,
		raw:    raw,
	}
}

// Retrieve goes to the configuration source and retrieves the selected data which
// contains the value to be injected in the configuration and the corresponding watcher that
// will be used to monitor for updates of the retrieved value.
//
// `uri` must follow the "<scheme>:<opaque_data>" format. This format is compatible
// with the URI definition (see https://datatracker.ietf.org/doc/html/rfc3986). The "<scheme>"
// must be always included in the `uri`. The "<scheme>" supported by any provider:
//   - MUST consist of a sequence of characters beginning with a letter and followed by any
//     combination of letters, digits, plus ("+"), period ("."), or hyphen ("-").
//     See https://datatracker.ietf.org/doc/html/rfc3986#section-3.1.
//   - MUST be at least 2 characters long to avoid conflicting with a driver-letter identifier as specified
//     in https://tools.ietf.org/id/draft-kerwin-file-scheme-07.html#syntax.
//   - For testing, all implementation MUST check that confmaptest.ValidateProviderScheme returns no error.
//
// `watcher` callback is called when the config changes. watcher may be called from
// a different go routine. After watcher is called Retrieved.Get should be called
// to get the new config. See description of Retrieved for more details.
// watcher may be nil, which indicates that the caller is not interested in
// knowing about the changes.
//
// If ctx is canceled should return immediately with an error.
// Should never be called concurrently with itself or with Shutdown.
func (m *mapProvider) Retrieve(context.Context, string, confmap.WatcherFunc) (*confmap.Retrieved, error) {
	return confmap.NewRetrieved(m.raw, []confmap.RetrievedOption{}...)
}

// Scheme returns the location scheme used by Retrieve.
func (m *mapProvider) Scheme() string {
	return m.scheme
}

// Shutdown signals that the configuration for which this Provider was used to
// retrieve values is no longer in use and the Provider should close and release
// any resources that it may have created.
//
// This method must be called when the Collector service ends, either in case of
// success or error. Retrieve cannot be called after Shutdown.
//
// Should never be called concurrently with itself or with Retrieve.
// If ctx is canceled should return immediately with an error.
func (m *mapProvider) Shutdown(context.Context) error {
	return nil
}
