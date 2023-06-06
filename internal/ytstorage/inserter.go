package ytstorage

import (
	"context"

	"github.com/go-faster/errors"
	"go.ytsaurus.tech/yt/go/yt"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Inserter = (*Inserter)(nil)

// Inserter implements tracestorage.Inserter based on YTSaurus.
type Inserter struct {
	yc     yt.Client
	tables Tables
}

// NewInserter creates new Inserter.
func NewInserter(yc yt.Client, tables Tables) *Inserter {
	return &Inserter{
		yc:     yc,
		tables: tables,
	}
}

// InsertSpans inserts given spans.
func (i *Inserter) InsertSpans(ctx context.Context, spans []tracestorage.Span) error {
	bw := i.yc.NewRowBatchWriter()

	for _, s := range spans {
		if err := bw.Write(s); err != nil {
			return errors.Wrap(err, "write span")
		}
	}

	if err := bw.Commit(); err != nil {
		return errors.Wrap(err, "commit")
	}
	var (
		update        = true
		insertOptions = &yt.InsertRowsOptions{
			Update: &update,
		}
	)
	return i.yc.InsertRowBatch(ctx, i.tables.spans, bw.Batch(), insertOptions)
}

// InsertTags insert given set of tags to the storage.
func (i *Inserter) InsertTags(ctx context.Context, tags map[tracestorage.Tag]struct{}) error {
	bw := i.yc.NewRowBatchWriter()

	for tag := range tags {
		if err := bw.Write(tag); err != nil {
			return errors.Wrap(err, "write tag")
		}
	}

	if err := bw.Commit(); err != nil {
		return errors.Wrap(err, "commit")
	}
	var (
		update        = true
		insertOptions = &yt.InsertRowsOptions{
			Update: &update,
		}
	)
	return i.yc.InsertRowBatch(ctx, i.tables.tags, bw.Batch(), insertOptions)
}
