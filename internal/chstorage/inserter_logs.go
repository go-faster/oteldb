package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/logstorage"
)

func (i *Inserter) mapRecords(c *logColumns, records []logstorage.Record) {
	for _, r := range records {
		c.AddRow(r)
	}
}

// InsertLogLabels inserts given set of labels to the storage.
func (i *Inserter) InsertLogLabels(context.Context, map[logstorage.Label]struct{}) error {
	// No-op.
	// TODO(ernado): do we really need this or can just use materialized view?
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

	return nil
}
