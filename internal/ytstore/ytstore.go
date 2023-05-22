// Package ytstore provide YTSaurus-based trace storage.
package ytstore

import (
	"context"
	"encoding/binary"
	"encoding/json"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson/yson2json"
	"go.ytsaurus.tech/yt/go/yt"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/otelreceiver"
)

var _ otelreceiver.Handler = (*Store)(nil)

// Store implements ytsaurus-based trace storage.
type Store struct {
	yc    yt.Client
	table ypath.Path
}

// NewStore creates new Store.
func NewStore(yc yt.Client, table ypath.Path) *Store {
	return &Store{
		yc:    yc,
		table: table,
	}
}

// Span is a data structure for trace.
type Span struct {
	TraceID string               `yson:"trace_id"`
	SpanID  uint64               `yson:"id"`
	Start   uint64               `yson:"start"`
	End     uint64               `yson:"end"`
	Attrs   yson2json.RawMessage `yson:"attrs"`
}

// Schema returns table schema for this structure.
func (Span) Schema() schema.Schema {
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			// FIXME(tdakkota): where is UUID?
			{Name: "trace_id", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "id", ComplexType: schema.TypeUint64, SortOrder: schema.SortAscending},
			{Name: "start", ComplexType: schema.TypeTimestamp},
			{Name: "end", ComplexType: schema.TypeTimestamp},
			{Name: "attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},
		},
	}
}

// Migrate setups YTSaurus tables for storage.
func (s *Store) Migrate(ctx context.Context) error {
	tables := map[ypath.Path]migrate.Table{
		s.table: {
			Schema: Span{}.Schema(),
		},
	}
	if err := migrate.EnsureTables(ctx, s.yc, tables, migrate.OnConflictFail); err != nil {
		return errors.Wrap(err, "ensure tables")
	}
	return nil
}

func getSpanID(span ptrace.Span) uint64 {
	arr := span.SpanID()
	return binary.LittleEndian.Uint64(arr[:])
}

// ConsumeTraces implements otelreceiver.Handler.
func (s *Store) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	bw := s.yc.NewRowBatchWriter()

	resSpans := traces.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		resSpan := resSpans.At(i)

		scopeSpans := resSpan.ScopeSpans()
		for i := 0; i < scopeSpans.Len(); i++ {
			scopeSpan := scopeSpans.At(i)

			spans := scopeSpan.Spans()
			for i := 0; i < spans.Len(); i++ {
				span := spans.At(i)

				traceID := span.TraceID()
				spanID := getSpanID(span)

				attrs, err := json.Marshal(span.Attributes().AsRaw())
				if err != nil {
					return errors.Wrapf(err, "marshal %s:%d attributes", traceID, spanID)
				}

				s := Span{
					TraceID: span.TraceID().String(),
					SpanID:  spanID,
					Start:   uint64(span.StartTimestamp()),
					End:     uint64(span.EndTimestamp()),
					Attrs: yson2json.RawMessage{
						JSON: attrs,
					},
				}

				if err := bw.Write(s); err != nil {
					return errors.Wrap(err, "write row")
				}
			}
		}
	}

	if err := bw.Commit(); err != nil {
		return errors.Wrap(err, "commit")
	}
	if err := s.yc.InsertRowBatch(ctx, s.table, bw.Batch(), &yt.InsertRowsOptions{}); err != nil {
		return errors.Wrap(err, "insert rows")
	}
	return nil
}
