package ytstorage

import (
	"context"

	"github.com/go-faster/errors"
	"go.ytsaurus.tech/yt/go/yt"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var (
	_ tracestorage.Inserter = (*Inserter)(nil)
	_ logstorage.Inserter   = (*Inserter)(nil)
)

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

func insertSlice[T any](ctx context.Context, i *Inserter, data []T) error {
	bw := i.yc.NewRowBatchWriter()

	for _, e := range data {
		if err := bw.Write(e); err != nil {
			return errors.Wrapf(err, "write %T", e)
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

func insertSet[T comparable](ctx context.Context, i *Inserter, data map[T]struct{}) error {
	bw := i.yc.NewRowBatchWriter()

	for k := range data {
		if err := bw.Write(k); err != nil {
			return errors.Wrapf(err, "write %T", k)
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

// InsertSpans inserts given spans.
func (i *Inserter) InsertSpans(ctx context.Context, spans []tracestorage.Span) error {
	return insertSlice(ctx, i, spans)
}

// InsertTags insert given set of tags to the storage.
func (i *Inserter) InsertTags(ctx context.Context, tags map[tracestorage.Tag]struct{}) error {
	return insertSet(ctx, i, tags)
}

// InsertRecords inserts given Records.
func (i *Inserter) InsertRecords(ctx context.Context, records []logstorage.Record) error {
	return insertSlice(ctx, i, records)
}

// InsertLogLabels insert given set of labels to the storage.
func (i *Inserter) InsertLogLabels(ctx context.Context, labels map[logstorage.Label]struct{}) error {
	return insertSet(ctx, i, labels)
}
