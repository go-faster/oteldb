package metricsharding

import (
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
)

// TenantID is a tenant ID.
type TenantID = int64

// Block is a metric points/attributes block.
type Block struct {
	// Root is a root path.
	Root ypath.Path
	// At is a time block starting point.
	At time.Time
	// Tenant is tenant id of block.
	Tenant TenantID
}

// Resource returns resource table path.
func (s Block) Resource() ypath.Path {
	return s.Root.Child("resource")
}

// Attributes returns attributes table path.
func (s Block) Attributes() ypath.Path {
	return s.Root.Child("attributes")
}

// Points returns points table path.
func (s Block) Points() ypath.Path {
	return s.Root.Child("points")
}

func newBlock(root ypath.Path, tenant TenantID, at time.Time) Block {
	return Block{
		root,
		at,
		tenant,
	}
}

// QueryBlocks describes block to query.
type QueryBlocks struct {
	// Active is a list of dynamic tables with recent points.
	Active []ypath.Path
	// RecentAttributes is a list of block with recent attributes.
	RecentAttributes []Block
	// RecentResource is a list of block with recent resources.
	RecentResource []Block
	// Closed blocks with static tables.
	Closed []Block
}
