package metricsharding

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/metricstorage"
)

// Sharder controls sharding.
type Sharder struct {
	yc yt.Client

	shardOpts ShardingOptions
}

// NewSharder creates new [Sharder].
func NewSharder(yc yt.Client, shardOpts ShardingOptions) *Sharder {
	shardOpts.SetDefaults()

	return &Sharder{
		yc:        yc,
		shardOpts: shardOpts,
	}
}

const timeBlockLayout = "2006-01-02_15-04-05"

func (s *Sharder) tenantPath(id TenantID) ypath.Path {
	return s.shardOpts.Root.Child(fmt.Sprintf("tenant_%v", id))
}

func (s *Sharder) currentBlockStart() time.Time {
	return time.Now().UTC().Truncate(s.shardOpts.BlockDelta)
}

// CreateTenant creates storage strucute for given tenant.
func (s *Sharder) CreateTenant(ctx context.Context, tenant TenantID, at time.Time) error {
	var (
		activePath    = s.tenantPath(tenant).Child("active")
		timePartition = at.UTC().Truncate(s.shardOpts.AttributeDelta).Format(timeBlockLayout)
	)
	return migrate.EnsureTables(ctx, s.yc,
		map[ypath.Path]migrate.Table{
			activePath.Child("resource").Child(timePartition): {
				Schema: metricstorage.Resource{}.YTSchema(),
			},
			activePath.Child("attributes").Child(timePartition): {
				Schema: metricstorage.Attributes{}.YTSchema(),
			},
			activePath.Child("points"): {
				Schema: metricstorage.Point{}.YTSchema(),
			},
		},
		migrate.OnConflictTryAlter(ctx, s.yc),
	)
}

// GetBlocksForQuery returns list of blocks to query.
func (s *Sharder) GetBlocksForQuery(ctx context.Context, tenants []TenantID, start, end time.Time) (qb QueryBlocks, _ error) {
	var (
		currentBlockStart = s.currentBlockStart()

		// Query closed blocks only if range includes points before start of the active block.
		needClosed = start.Before(currentBlockStart)
		// Query current blocks only if range includes points after start of the active block.
		needActive = end.After(currentBlockStart)
	)

	var (
		attributeMux sync.Mutex
		closedMux    sync.Mutex
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	for _, tenant := range tenants {
		tenant := tenant
		tenantPath := s.tenantPath(tenant)

		if needActive {
			activePath := tenantPath.Child("active")
			qb.Active = append(qb.Active, activePath.Child("points"))

			grp.Go(func() error {
				ctx := grpCtx

				blocks, err := s.getBlocks(ctx, activePath.Child("attributes"), tenant, start, end)
				if err != nil {
					return errors.Wrapf(err, "get attributes block for tenant %v", tenant)
				}

				attributeMux.Lock()
				qb.RecentAttributes = append(qb.RecentAttributes, blocks...)
				attributeMux.Unlock()
				return nil
			})
			grp.Go(func() error {
				ctx := grpCtx

				blocks, err := s.getBlocks(ctx, activePath.Child("resource"), tenant, start, end)
				if err != nil {
					return errors.Wrapf(err, "get resource block for tenant %v", tenant)
				}

				attributeMux.Lock()
				qb.RecentResource = append(qb.RecentResource, blocks...)
				attributeMux.Unlock()
				return nil
			})
		}
		if needClosed {
			closedPath := tenantPath.Child("closed")
			grp.Go(func() error {
				ctx := grpCtx

				blocks, err := s.getBlocks(ctx, closedPath, tenant, start, end)
				if err != nil {
					return errors.Wrapf(err, "get closed block for tenant %v", tenant)
				}

				closedMux.Lock()
				qb.Closed = append(qb.Closed, blocks...)
				closedMux.Unlock()
				return nil
			})
		}
	}
	if err := grp.Wait(); err != nil {
		return qb, err
	}
	return QueryBlocks{}, nil
}

func (s *Sharder) getBlocks(ctx context.Context,
	dir ypath.Path,
	tenant TenantID,
	start, end time.Time,
) ([]Block, error) {
	var (
		lg = zctx.From(ctx)

		dirs []string
	)
	if err := s.yc.ListNode(ctx, dir, &dirs, &yt.ListNodeOptions{}); err != nil {
		return nil, errors.Wrapf(err, "get %q dirs", dir)
	}
	if len(dirs) == 0 {
		// Tenant has no data.
		return nil, nil
	}

	blocks := make([]timeBlock, 0, len(dirs))
	for _, dir := range dirs {
		t, err := time.Parse(timeBlockLayout, dir)
		if err != nil {
			lg.Warn("Invalid time block format", zap.String("block_dir", dir))
			continue
		}
		blocks = append(blocks, timeBlock{
			start: t,
			dir:   dir,
		})
	}

	var result []Block
	for _, block := range timeBlocksForRange(blocks, start, end) {
		result = append(result,
			newBlock(dir.Child(block.dir), tenant, block.start),
		)
	}
	return result, nil
}

type timeBlock struct {
	start time.Time
	end   time.Time
	dir   string
}

func timeBlocksForRange(blocks []timeBlock, start, end time.Time) []timeBlock {
	if len(blocks) == 0 {
		return blocks
	}

	// Sort blocks in ascending order.
	slices.SortFunc(blocks, func(a, b timeBlock) int {
		return a.start.Compare(b.start)
	})
	for i := range blocks {
		if i < len(blocks)-1 {
			next := blocks[i+1]
			blocks[i].end = next.start
		}
	}

	// Find the leftmost block.
	if !start.IsZero() {
		for idx, block := range blocks {
			if block.end.IsZero() || block.end.After(start) {
				blocks = blocks[idx:]
				break
			}
		}
	}

	// Find the rightmost block.
	if !end.IsZero() {
		for idx, block := range blocks {
			if block.start.After(end) {
				blocks = blocks[:idx]
				break
			}
		}
	}

	return blocks
}
