package ytstorage

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.ytsaurus.tech/yt/go/ypath"
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
	tracer trace.Tracer
}

// InserterOptions is Inserter's options.
type InserterOptions struct {
	// Tables provides table paths to query.
	Tables Tables
	// MeterProvider provides OpenTelemetry meter for this querier.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer for this querier.
	TracerProvider trace.TracerProvider
}

func (opts *InserterOptions) setDefaults() {
	if opts.Tables == (Tables{}) {
		opts.Tables = defaultTables
	}
	if opts.MeterProvider == nil {
		opts.MeterProvider = otel.GetMeterProvider()
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}
}

// NewInserter creates new Inserter.
func NewInserter(yc yt.Client, opts InserterOptions) (*Inserter, error) {
	return &Inserter{
		yc:     yc,
		tables: opts.Tables,
		tracer: opts.TracerProvider.Tracer("ytstorage.Inserter"),
	}, nil
}

func insertSlice[T any](ctx context.Context, i *Inserter, table ypath.Path, data []T) error {
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
	return i.yc.InsertRowBatch(ctx, table, bw.Batch(), insertOptions)
}

func insertSet[T comparable](ctx context.Context, i *Inserter, table ypath.Path, data map[T]struct{}) error {
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
	return i.yc.InsertRowBatch(ctx, table, bw.Batch(), insertOptions)
}

// InsertSpans inserts given spans.
func (i *Inserter) InsertSpans(ctx context.Context, spans []tracestorage.Span) (rerr error) {
	table := i.tables.spans
	ctx, span := i.tracer.Start(ctx, "InsertSpans", trace.WithAttributes(
		attribute.Int("ytstorage.spans_count", len(spans)),
		attribute.Stringer("ytstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	return insertSlice(ctx, i, table, spans)
}

// InsertTags insert given set of tags to the storage.
func (i *Inserter) InsertTags(ctx context.Context, tags map[tracestorage.Tag]struct{}) (rerr error) {
	table := i.tables.tags
	ctx, span := i.tracer.Start(ctx, "InsertTags", trace.WithAttributes(
		attribute.Int("ytstorage.tags_count", len(tags)),
		attribute.Stringer("ytstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	return insertSet(ctx, i, table, tags)
}

// InsertRecords inserts given Records.
func (i *Inserter) InsertRecords(ctx context.Context, records []logstorage.Record) (rerr error) {
	table := i.tables.logs
	ctx, span := i.tracer.Start(ctx, "InsertRecords", trace.WithAttributes(
		attribute.Int("ytstorage.records_count", len(records)),
		attribute.Stringer("ytstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	return insertSlice(ctx, i, table, records)
}

// InsertLogLabels insert given set of labels to the storage.
func (i *Inserter) InsertLogLabels(ctx context.Context, labels map[logstorage.Label]struct{}) (rerr error) {
	table := i.tables.logLabels
	ctx, span := i.tracer.Start(ctx, "InsertLogLabels", trace.WithAttributes(
		attribute.Int("ytstorage.labels_count", len(labels)),
		attribute.Stringer("ytstorage.table", table),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	return insertSet(ctx, i, table, labels)
}
