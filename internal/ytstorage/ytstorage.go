// Package ytstorage provides YTSaurus-based storage.
package ytstorage

import (
	"context"

	"github.com/go-faster/errors"
	"go.ytsaurus.tech/yt/go/migrate"
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

// Migrate setups YTSaurus tables for storage.
func (s *Tables) Migrate(ctx context.Context, yc yt.Client, onConflict migrate.ConflictFn) error {
	// FIXME(tdakkota): this package should not rely on schema from tracestorage.
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
	if err := migrate.EnsureTables(ctx, yc, tables, onConflict); err != nil {
		return errors.Wrap(err, "ensure tables")
	}
	return nil
}
