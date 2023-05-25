// Package ytstore provide YTSaurus-based trace storage.
package ytstore

import (
	"context"
	"encoding/binary"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
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
	TraceID       string  `yson:"trace_id"`
	SpanID        uint64  `yson:"span_id"`
	TraceState    string  `yson:"trace_state"`
	ParentSpanID  *uint64 `yson:"parent_span_id"`
	Name          string  `yson:"name"`
	Kind          int32   `yson:"kind"`
	Start         uint64  `yson:"start"`
	End           uint64  `yson:"end"`
	Attrs         Attrs   `yson:"attrs"`
	StatusCode    int32   `yson:"status_code"`
	StatusMessage string  `yson:"status_message"`

	BatchID       string `yson:"batch_id"`
	ResourceAttrs Attrs  `yson:"resource_attrs"`

	ScopeName    string `yson:"scope_name"`
	ScopeVersion string `yson:"scope_version"`
	ScopeAttrs   Attrs  `yson:"scope_attrs"`
}

// Schema returns table schema for this structure.
func (Span) Schema() schema.Schema {
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			// FIXME(tdakkota): where is UUID?
			{Name: "trace_id", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "span_id", ComplexType: schema.TypeUint64, SortOrder: schema.SortAscending},
			{Name: "trace_state", ComplexType: schema.TypeString},
			{Name: "parent_span_id", ComplexType: schema.Optional{Item: schema.TypeUint64}},
			{Name: "name", ComplexType: schema.TypeString},
			{Name: "kind", ComplexType: schema.TypeInt32},
			// Start and end are nanoseconds, so we can't use Timestamp.
			{Name: "start", ComplexType: schema.TypeUint64},
			{Name: "end", ComplexType: schema.TypeUint64},
			{Name: "attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},
			{Name: "status_code", ComplexType: schema.TypeInt32},
			{Name: "status_message", ComplexType: schema.TypeString},

			{Name: "batch_id", ComplexType: schema.TypeString},
			{Name: "resource_attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},

			{Name: "scope_name", ComplexType: schema.TypeString},
			{Name: "scope_version", ComplexType: schema.TypeString},
			{Name: "scope_attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},
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

func otelToYTSpan(
	batchID string,
	res pcommon.Resource,
	scope pcommon.InstrumentationScope,
	span ptrace.Span,
) (s Span) {
	getSpanID := func(arr pcommon.SpanID) uint64 {
		return binary.LittleEndian.Uint64(arr[:])
	}

	status := span.Status()
	s = Span{
		TraceID:       span.TraceID().String(),
		SpanID:        getSpanID(span.SpanID()),
		TraceState:    span.TraceState().AsRaw(),
		ParentSpanID:  nil,
		Name:          span.Name(),
		Kind:          int32(span.Kind()),
		Start:         uint64(span.StartTimestamp()),
		End:           uint64(span.EndTimestamp()),
		Attrs:         Attrs(span.Attributes()),
		StatusCode:    int32(status.Code()),
		StatusMessage: status.Message(),
		BatchID:       batchID,
		ResourceAttrs: Attrs(res.Attributes()),
		ScopeName:     scope.Name(),
		ScopeVersion:  scope.Version(),
		ScopeAttrs:    Attrs(scope.Attributes()),
	}
	if parent := span.ParentSpanID(); !parent.IsEmpty() {
		v := getSpanID(parent)
		s.ParentSpanID = &v
	}

	return s
}

// ConsumeTraces implements otelreceiver.Handler.
func (s *Store) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	bw := s.yc.NewRowBatchWriter()

	resSpans := traces.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		batchID := uuid.New().String()
		resSpan := resSpans.At(i)
		res := resSpan.Resource()

		scopeSpans := resSpan.ScopeSpans()
		for i := 0; i < scopeSpans.Len(); i++ {
			scopeSpan := scopeSpans.At(i)
			scope := scopeSpan.Scope()

			spans := scopeSpan.Spans()
			for i := 0; i < spans.Len(); i++ {
				s := otelToYTSpan(batchID, res, scope, spans.At(i))
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
