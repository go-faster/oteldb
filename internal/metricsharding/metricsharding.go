// Package metricsharding contains YT metric storage implementation.
package metricsharding

import (
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.ytsaurus.tech/yt/go/ypath"
)

const timeBlockLayout = "2006-01-02_15-04-05"

// ShardingOptions sets sharding options.
type ShardingOptions struct {
	// Root path of storage.
	Root ypath.Path

	// ExtractTenant extracts TenantID from resource and attributes.
	ExtractTenant func(resource, attrs pcommon.Map) (TenantID, bool)

	// AttributeDelta defines partition (δ=1h) of the current block attributes.
	AttributeDelta time.Duration
	// BlockDelta defines partition (Δ=1d) of the closed blocks.
	BlockDelta time.Duration
}

// TenantPath returns root path for given tenant.
func (opts *ShardingOptions) TenantPath(id TenantID) ypath.Path {
	return opts.Root.Child(fmt.Sprintf("tenant_%v", id))
}

// SetDefaults sets default options.
func (opts *ShardingOptions) SetDefaults() {
	if opts.Root == "" {
		opts.Root = ypath.Path("//oteldb/metrics")
	}
	if opts.ExtractTenant == nil {
		opts.ExtractTenant = func(resource, attrs pcommon.Map) (id TenantID, _ bool) {
			v, ok := resource.Get("oteldb.tenant_id")
			if !ok {
				return id, false
			}
			switch v.Type() {
			case pcommon.ValueTypeInt:
				return v.Int(), true
			case pcommon.ValueTypeStr:
				p, err := strconv.ParseInt(v.Str(), 10, 64)
				return p, err == nil
			default:
				return id, false
			}
		}
	}
	if opts.AttributeDelta == 0 {
		opts.AttributeDelta = time.Hour
	}
	if opts.BlockDelta == 0 {
		opts.BlockDelta = 24 * time.Hour
	}
}
