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
		tenantPath        = s.tenantPath(tenant)
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

	blocks, err := s.getBlocks(ctx, activePath, start, end)
	if err != nil {
		return errors.Wrap(err, "get attribute blocks to merge")
	}

	opSpec := spec.Merge()
	for _, block := range blocks {
		opSpec = opSpec.AddInput(block.Root)
	}
	opSpec.OutputTablePath = targetPath

	lg := zctx.From(ctx)
	op, err := s.mapreduce.Merge(opSpec)
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

	return nil
}

func (s *Sharder) archivePoints(ctx context.Context,
	tenantPath ypath.Path,
	start, end time.Time,
) (rerr error) {
	const table = "points"
	var (
		activePath = tenantPath.Child("active").Child(table)
		targetPath = tenantPath.Child("closed").Child(start.Format(timeBlockLayout)).Child(table)
	)

	if _, err := yt.CreateTable(ctx, s.yc, targetPath,
		yt.WithSchema(metricstorage.Point{}.YTSchema()),
		yt.WithRecursive(),
	); err != nil {
		return errors.Wrapf(err, "create static table %q", targetPath)
	}

	opSpec := spec.Merge()
	opSpec.InputTablePaths = []ypath.YPath{activePath}
	opSpec.OutputTablePath = targetPath
	opSpec.InputQuery = fmt.Sprintf(
		"* FROM [%s] WHERE timestamp >= %d AND timestamp < %d",
		activePath, start.UnixNano(), end.UnixNano(),
	)

	lg := zctx.From(ctx)
	op, err := s.mapreduce.Merge(opSpec)
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

	return nil
}
