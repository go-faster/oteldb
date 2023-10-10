package metricsharding

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"go.ytsaurus.tech/yt/go/ypath"
)

// TenantID is a tenant ID.
type TenantID int64

// ParseTenant parses dir name to tenant.
func ParseTenant(s string) (TenantID, error) {
	id, ok := strings.CutPrefix(s, "tenant-")
	if !ok {
		return 0, errors.Errorf("invalid tenant name %q: expected prefix", s)
	}
	v, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return 0, err
	}
	return TenantID(v), nil
}

// UnmarshalText implements [encoding.TextUnmarshaler].
func (id *TenantID) UnmarshalText(data []byte) error {
	v, err := ParseTenant(string(data))
	if err != nil {
		return err
	}
	*id = v
	return nil
}

// String implements [fmt.Stringer].
func (id TenantID) String() string {
	return fmt.Sprintf("tenant-%d", int64(id))
}

// MarshalText implements [encoding.TextMarshaler].
func (id TenantID) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "tenant-%d", int64(id)), nil
}

// Block is a metric points/attributes block.
type Block struct {
	// Root is a root path.
	Root ypath.Path
	// At is a time block starting point.
	At time.Time
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

func newBlock(root ypath.Path, at time.Time) Block {
	return Block{
		root,
		at,
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
