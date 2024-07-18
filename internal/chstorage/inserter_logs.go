package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/typedpool"
)

type recordWriter struct {
	logs     *logColumns
	attrs    *logAttrMapColumns
	inserter *Inserter
}

var _ logstorage.RecordWriter = (*recordWriter)(nil)

// Add adds record to the batch.
func (w *recordWriter) Add(record logstorage.Record) error {
	w.logs.AddRow(record)
	w.attrs.AddAttrs(record.Attrs)
	w.attrs.AddAttrs(record.ResourceAttrs)
	w.attrs.AddAttrs(record.ScopeAttrs)
	return nil
}

// Submit sends batch.
func (w *recordWriter) Submit(ctx context.Context) error {
	return w.inserter.submitLogs(ctx, w.logs, w.attrs)
}

// Close frees resources.
func (w *recordWriter) Close() error {
	logColumnsPool.Put(w.logs)
	logAttrMapColumnsPool.Put(w.attrs)
	return nil
}

var _ logstorage.Inserter = (*Inserter)(nil)

// RecordWriter returns a new [logstorage.RecordWriter]
func (i *Inserter) RecordWriter(ctx context.Context) (logstorage.RecordWriter, error) {
	logs := typedpool.GetReset(logColumnsPool)
	attrs := typedpool.GetReset(logAttrMapColumnsPool)

	return &recordWriter{
		logs:     logs,
		attrs:    attrs,
		inserter: i,
	}, nil
}

func (i *Inserter) submitLogs(ctx context.Context, logs *logColumns, attrs *logAttrMapColumns) (rerr error) {
	table := i.tables.Logs
	ctx, span := i.tracer.Start(ctx, "chstorage.logs.submitLogs", trace.WithAttributes(
		attribute.Int("chstorage.records_count", logs.body.Rows()),
		attribute.Int("chstorage.attrs_count", attrs.name.Rows()),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			i.stats.InsertedRecords.Add(ctx, int64(logs.body.Rows()))
			i.stats.InsertedLogLabels.Add(ctx, int64(attrs.name.Rows()))

			i.stats.Inserts.Add(ctx, 1,
				metric.WithAttributes(
					attribute.String("chstorage.signal", "logs"),
				),
			)
		}
		span.End()
	}()

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx

		input := logs.Input()
		if err := i.ch.Do(ctx, ch.Query{
			Logger: zctx.From(ctx).Named("ch"),
			Body:   input.Into(table),
			Input:  input,
		}); err != nil {
			return errors.Wrap(err, "insert records")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx

		input := attrs.Input()
		if err := i.ch.Do(ctx, ch.Query{
			Logger: zctx.From(ctx).Named("ch"),
			Body:   input.Into(i.tables.LogAttrs),
			Input:  input,
		}); err != nil {
			return errors.Wrap(err, "insert labels")
		}
		return nil
	})
	return grp.Wait()
}
