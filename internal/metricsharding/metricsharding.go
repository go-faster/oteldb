// Package metricsharding contains YT metric storage implementation.
package metricsharding

import (
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/go-faster/oteldb/internal/metricstorage"
)

const timeBlockLayout = "2006-01-02_15-04-05"

// ShardingOptions sets sharding options.
type ShardingOptions struct {
	// Root path of storage.
	Root ypath.Path

	// TenantFromAttrs extracts TenantID from resource and attributes.
	TenantFromAttrs func(resource, attrs pcommon.Map) (TenantID, bool)
	// TenantFromLabels extracts TenantIDs from labels.
	TenantFromLabels func(ctx context.Context, s *Sharder, labels []*labels.Matcher) ([]TenantID, error)

	// AttributeDelta defines partition (δ=1h) of the current block attributes.
	AttributeDelta time.Duration
	// BlockDelta defines partition (Δ=1d) of the closed blocks.
	BlockDelta time.Duration
}

// TenantPath returns root path for given tenant.
func (opts *ShardingOptions) TenantPath(id TenantID) ypath.Path {
	opts.SetDefaults()

	return opts.Root.Child(id.String())
}

// CurrentBlockStart returns current block start point.
func (opts *ShardingOptions) CurrentBlockStart() time.Time {
	opts.SetDefaults()

	return time.Now().UTC().Truncate(opts.BlockDelta)
}

// CreateTenant creates storage strucute for given tenant.
func (opts *ShardingOptions) CreateTenant(ctx context.Context, yc yt.Client, tenant TenantID, at time.Time) error {
	opts.SetDefaults()

	var (
		activePath    = opts.TenantPath(tenant).Child("active")
		timePartition = at.UTC().Truncate(opts.AttributeDelta).Format(timeBlockLayout)
		attrs         = map[string]any{
			"optimize_for": "scan",

			// HACK: probably tenant id newtype should be reworked.
			"tenant_id": int64(tenant),
		}
	)
	return migrate.EnsureTables(ctx, yc,
		map[ypath.Path]migrate.Table{
			activePath.Child("resource").Child(timePartition): {
				Schema:     metricstorage.Resource{}.YTSchema(),
				Attributes: attrs,
			},
			activePath.Child("attributes").Child(timePartition): {
				Schema:     metricstorage.Attributes{}.YTSchema(),
				Attributes: attrs,
			},
			activePath.Child("points"): {
				Schema:     metricstorage.Point{}.YTSchema(),
				Attributes: attrs,
			},
		},
		migrate.OnConflictTryAlter(ctx, yc),
	)
}

// SetDefaults sets default options.
func (opts *ShardingOptions) SetDefaults() {
	if opts.Root == "" {
		opts.Root = ypath.Path("//oteldb/metrics")
	}
	if opts.TenantFromAttrs == nil {
		opts.TenantFromAttrs = func(resource, attrs pcommon.Map) (id TenantID, _ bool) {
			v, ok := resource.Get("oteldb.tenant_id")
			if !ok {
				return id, false
			}
			switch v.Type() {
			case pcommon.ValueTypeInt:
				return TenantID(v.Int()), true
			case pcommon.ValueTypeStr:
				v, err := strconv.ParseInt(v.Str(), 10, 64)
				return TenantID(v), err == nil
			default:
				return id, false
			}
		}
	}
	if opts.TenantFromLabels == nil {
		opts.TenantFromLabels = DefaultTenantsFromLabels
	}
	if opts.AttributeDelta == 0 {
		opts.AttributeDelta = time.Hour
	}
	if opts.BlockDelta == 0 {
		opts.BlockDelta = 24 * time.Hour
	}
}

// DefaultTenantsFromLabels defines default implementation of TenantFromLabels.
func DefaultTenantsFromLabels(ctx context.Context, s *Sharder, matchers []*labels.Matcher) ([]TenantID, error) {
	const labelName = "oteldb_tenant"

	var tenantMatchers []*labels.Matcher
	for _, m := range matchers {
		if m.Name != labelName {
			continue
		}

		switch m.Type {
		case labels.MatchEqual, labels.MatchRegexp:
		default:
			return nil, errors.Errorf("unsupported matcher: %q", m)
		}

		tenantMatchers = append(tenantMatchers, m)
	}

	tenants, err := s.ListTenants(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list tenants")
	}

	// Filter in-place by given predicates.
	keep := func(tenant TenantID) bool {
		str := tenant.String()
		for _, m := range tenantMatchers {
			if m.Matches(str) {
				return true
			}
		}
		return false
	}
	n := 0
	for _, tenant := range tenants {
		if !keep(tenant) {
			continue
		}
		tenants[n] = tenant
	}
	tenants = tenants[:n]
	return tenants, nil
}
