// Package metricsharding contains YT metric storage implementation.
package metricsharding

import (
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
)

// ShardingOptions sets sharding options.
type ShardingOptions struct {
	// Root path of storage.
	Root ypath.Path

	// AttributeDelta defines partition (δ=1h) of the current block attributes.
	AttributeDelta time.Duration
	// BlockDelta defines partition (Δ=1d) of the closed blocks.
	BlockDelta time.Duration
}

// SetDefaults sets default options.
func (opts *ShardingOptions) SetDefaults() {
	if opts.Root == "" {
		opts.Root = ypath.Path("//oteldb/metrics")
	}
	if opts.AttributeDelta == 0 {
		opts.AttributeDelta = time.Hour
	}
	if opts.BlockDelta == 0 {
		opts.BlockDelta = 24 * time.Hour
	}
}
