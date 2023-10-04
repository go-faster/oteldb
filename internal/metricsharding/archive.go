package metricsharding

import (
	"context"
	"fmt"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"

	"github.com/go-faster/oteldb/internal/metricstorage"
)

// ArchiveTenant creates a new closed block, if needed.
func (s *Sharder) ArchiveTenant(ctx context.Context, tenant TenantID) (rerr error) {
	var (
		tenantPath        = s.shardOpts.TenantPath(tenant)
		currentBlockStart = s.currentBlockStart()

		start = currentBlockStart.Add(-s.shardOpts.BlockDelta)
		end   = currentBlockStart
	)

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		return s.archivePoints(ctx, tenantPath, start, end)
	})
	for _, block := range []struct {
		Name   string
		Schema schema.Schema
	}{
		{"attributes", metricstorage.Attributes{}.YTSchema()},
		{"resource", metricstorage.Resource{}.YTSchema()},
	} {
		block := block
		grp.Go(func() error {
			ctx := grpCtx
			return s.archiveAttributes(ctx, block.Name, block.Schema, tenantPath, start, end)
		})
	}

	return grp.Wait()
}

const defaultMergeMode = "ordered"

func (s *Sharder) archiveAttributes(ctx context.Context,
	dir string, targetSchema schema.Schema,
	tenantPath ypath.Path,
	start, end time.Time,
) error {
	var (
		activePath = tenantPath.Child("active").Child(dir)
		targetPath = tenantPath.Child("closed").Child(start.Format(timeBlockLayout)).Child(dir)
	)

	if _, err := yt.CreateTable(ctx, s.yc, targetPath,
		yt.WithSchema(targetSchema),
		yt.WithRecursive(),
	); err != nil {
		return errors.Wrapf(err, "create static table %q", targetPath)
	}

	lg := zctx.From(ctx)

	blocks, err := s.getBlocks(ctx, activePath, start, end)
	if err != nil {
		return errors.Wrap(err, "get attribute blocks to merge")
	}
	if len(blocks) == 0 {
		// No blocks to merge.
		lg.Info("No blocks to merge", zap.Stringer("to", targetPath))
		return nil
	}

	opSpec := spec.Merge()
	opSpec.MergeMode = defaultMergeMode
	for _, block := range blocks {
		opSpec = opSpec.AddInput(block.Root)
	}
	opSpec.OutputTablePath = targetPath

	op, err := s.mapreduce.Merge(opSpec)
	if err != nil {
		return errors.Wrap(err, "run merge operation")
	}
	lg.Info("Run attribute merge operation",
		zap.Stringer("id", op.ID()),
		zap.Stringer("from", activePath),
		zap.Stringer("to", targetPath),
	)

	if err := op.Wait(); err != nil {
		return errors.Wrapf(err, "wait operation %q", op.ID())
	}
	lg.Info("Merge operation done", zap.Stringer("id", op.ID()))

	if err := batchRemove(ctx, s.yc, opSpec.InputTablePaths...); err != nil {
		return errors.Wrap(err, "remove closed block attributes")
	}
	return nil
}

func batchRemove[T ypath.YPath](ctx context.Context, yc yt.CypressClient, paths ...T) error {
	var grp errgroup.Group
	for _, p := range paths {
		p := p
		grp.Go(func() error {
			return yc.RemoveNode(ctx, p, nil)
		})
	}
	return grp.Wait()
}

func (s *Sharder) archivePoints(ctx context.Context,
	tenantPath ypath.Path,
	start, end time.Time,
) (rerr error) {
	const table = "points"
	var (
		activePath = tenantPath.Child("active").Child(table)
		targetPath = tenantPath.Child("closed").Child(start.Format(timeBlockLayout)).Child(table)

		lg = zctx.From(ctx)
	)

	if _, err := yt.CreateTable(ctx, s.yc, targetPath,
		yt.WithSchema(metricstorage.Point{}.YTSchema()),
		yt.WithRecursive(),
		yt.WithForce(),
	); err != nil {
		return errors.Wrapf(err, "create static table %q", targetPath)
	}

	{
		mergeSpec := spec.Merge()
		mergeSpec.MergeMode = defaultMergeMode
		mergeSpec.InputTablePaths = []ypath.YPath{activePath}
		mergeSpec.OutputTablePath = targetPath
		mergeSpec.InputQuery = fmt.Sprintf(
			"* WHERE timestamp >= %d AND timestamp < %d",
			start.UnixNano(), end.UnixNano(),
		)

		op, err := s.mapreduce.Merge(mergeSpec)
		if err != nil {
			return errors.Wrap(err, "run merge operation")
		}
		lg.Info("Run merge operation",
			zap.Stringer("id", op.ID()),
			zap.Stringer("from", activePath),
			zap.Stringer("to", targetPath),
		)

		if err := op.Wait(); err != nil {
			return errors.Wrapf(err, "wait operation %q", op.ID())
		}
		lg.Info("Merge operation done", zap.Stringer("id", op.ID()))
	}

	{
		// See https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/bulk-insert#delete-where-via-input-query.
		truncateSpec := spec.Merge()
		truncateSpec.MergeMode = defaultMergeMode
		truncateSpec.InputTablePaths = []ypath.YPath{
			// Set attributes as guide says.
			`<append=%true; schema_modification="unversioned_update">` + activePath,
		}
		truncateSpec.OutputTablePath = activePath
		// Truncate all rows older than current block start.
		truncateSpec.InputQuery = fmt.Sprintf("* WHERE timestamp >= %d", end.UnixNano())

		op, err := s.mapreduce.Merge(truncateSpec)
		if err != nil {
			return errors.Wrap(err, "run truncate operation")
		}
		lg.Info("Run truncate operation",
			zap.Stringer("id", op.ID()),
			zap.Stringer("table", activePath),
		)

		if err := op.Wait(); err != nil {
			return errors.Wrapf(err, "wait operation %q", op.ID())
		}
		lg.Info("Truncate operation done", zap.Stringer("id", op.ID()))
	}

	return nil
}
