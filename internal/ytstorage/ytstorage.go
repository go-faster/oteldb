// Package ytstorage provides YTSaurus-based storage.
package ytstorage

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

// Tables define table paths.
type Tables struct {
	spans     ypath.Path
	tags      ypath.Path
	logs      ypath.Path
	logLabels ypath.Path

	// static whether to use static tables.
	static bool
}

var defaultTables = NewTables(ypath.Path("//oteldb"))

// NewTables creates new Tables with given path prefix.
func NewTables(prefix ypath.Path) Tables {
	traces := prefix.Child("traces")
	logs := prefix.Child("logs")
	return Tables{
		spans:     traces.Child("spans"),
		tags:      traces.Child("tags"),
		logs:      logs.Child("records"),
		logLabels: logs.Child("labels"),
	}
}

// NewStaticTables creates new Tables with given path prefix.
func NewStaticTables(prefix ypath.Path) Tables {
	t := NewTables(prefix)
	t.static = true
	return t
}

func (s *Tables) isStatic(p ypath.Path) bool {
	return s.static && p == s.logs
}

// Migrate setups YTSaurus tables for storage.
func (s *Tables) Migrate(ctx context.Context, yc yt.Client, onConflict migrate.ConflictFn) error {
	tables := map[ypath.Path]migrate.Table{
		s.spans: {
			Schema: tracestorage.Span{}.YTSchema(),
		},
		s.tags: {
			Schema: tracestorage.Tag{}.YTSchema(),
		},
		s.logs: {
			Schema: logstorage.Record{}.YTSchema(),
		},
		s.logLabels: {
			Schema: logstorage.Label{}.YTSchema(),
		},
	}

	staticTables := map[ypath.Path]migrate.Table{}
	for p, table := range tables {
		if s.isStatic(p) {
			// Add table to static tables map and remove from dynamic tables map.
			staticTables[p] = table
			// Do not use sorted columns to avoid 'sort order violation' errors.
			columns := table.Schema.Columns
			for i := range columns {
				columns[i].SortOrder = schema.SortNone
			}
			delete(tables, p)
		}
	}

	if err := migrateStaticTable(ctx, yc, staticTables); err != nil {
		return errors.Wrap(err, "ensure static tables")
	}
	if err := migrate.EnsureTables(ctx, yc, tables, onConflict); err != nil {
		return errors.Wrap(err, "ensure dynamic tables")
	}
	return nil
}

func migrateStaticTable(ctx context.Context, yc yt.Client, tables map[ypath.Path]migrate.Table) error {
	for path, table := range tables {
		exist, err := yc.NodeExists(ctx, path, nil)
		if err != nil {
			return errors.Wrapf(err, "check table %q", path)
		}
		if exist {
			// TODO(ernado): check schema?
			zctx.From(ctx).Warn("table already exist", zap.Stringer("path", path))
			continue
		}

		if _, err := yt.CreateTable(ctx, yc, path,
			yt.WithSchema(table.Schema),
			yt.WithAttributes(table.Attributes),
			yt.WithRecursive(),
		); err != nil {
			return errors.Wrapf(err, "create static table %q", path)
		}
	}
	return nil
}
