package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func (i *Inserter) mapRecords(c *logColumns, records []logstorage.Record) {
	for _, r := range records {
		c.AddRow(r)
	}
}

// InsertLogLabels inserts given set of labels to the storage.
func (i *Inserter) InsertLogLabels(ctx context.Context, set map[logstorage.Label]struct{}) (rerr error) {
	table := i.tables.LogAttrs
	ctx, span := i.tracer.Start(ctx, "InsertLogLabels", trace.WithAttributes(
		attribute.Int("chstorage.labels_count", len(set)),
		attribute.String("chstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	attrs := newLogAttrMapColumns()
	for label := range set {
		name := otelstorage.KeyToLabel(label.Name)
		attrs.AddRow(name, label.Name)
	}
	if err := i.ch.Do(ctx, ch.Query{
		Body:  attrs.Input().Into(table),
		Input: attrs.Input(),
	}); err != nil {
		return errors.Wrap(err, "insert labels")
	}

	return nil
}

// InsertRecords inserts given records.
func (i *Inserter) InsertRecords(ctx context.Context, records []logstorage.Record) (rerr error) {
	table := i.tables.Logs
	ctx, span := i.tracer.Start(ctx, "InsertRecords", trace.WithAttributes(
		attribute.Int("chstorage.records_count", len(records)),
		attribute.String("chstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			i.insertedRecords.Add(ctx, int64(len(records)))
		}
		span.End()
	}()

	logs := newLogColumns()
	i.mapRecords(logs, records)

	if err := i.ch.Do(ctx, ch.Query{
		Body:  logs.Input().Into(table),
		Input: logs.Input(),
	}); err != nil {
		return errors.Wrap(err, "insert records")
	}

	attrs := newLogAttrMapColumns()
	for _, record := range records {
		attrs.AddAttrs(record.Attrs)
		attrs.AddAttrs(record.ResourceAttrs)
		attrs.AddAttrs(record.ScopeAttrs)
	}
	if err := i.ch.Do(ctx, ch.Query{
		Body:  attrs.Input().Into(i.tables.LogAttrs),
		Input: attrs.Input(),
	}); err != nil {
		return errors.Wrap(err, "insert labels")
	}

	return nil
}
