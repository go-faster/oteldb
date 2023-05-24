// Package ytstore provide YTSaurus-based trace storage.
package ytstore

import (
	"context"
	"encoding/binary"
	"encoding/json"

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
	TraceID      string         `yson:"trace_id"`
	SpanID       uint64         `yson:"span_id"`
	ParentSpanID *uint64        `yson:"parent_span_id"`
	Name         string         `yson:"name"`
	Kind         int32          `yson:"name"`
	Start        uint64         `yson:"start"`
	End          uint64         `yson:"end"`
	IntAttrs     Attrs[int64]   `yson:"int_attrs"`
	DoubleAttrs  Attrs[float64] `yson:"double_attrs"`
	StrAttrs     Attrs[string]  `yson:"str_attrs"`
	BytesAttrs   Attrs[[]byte]  `yson:"bytes_attrs"`
	Attrs        Attrs[[]byte]  `yson:"attrs"`
}

// Schema returns table schema for this structure.
func (Span) Schema() schema.Schema {
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			// FIXME(tdakkota): where is UUID?
			{Name: "trace_id", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "span_id", ComplexType: schema.TypeUint64, SortOrder: schema.SortAscending},
			{Name: "parent_span_id", ComplexType: schema.Optional{Item: schema.TypeUint64}},
			{Name: "name", ComplexType: schema.TypeString},
			{Name: "kind", ComplexType: schema.TypeInt32},
			// Start and end are nanoseconds, so we can't use Timestamp.
			{Name: "start", ComplexType: schema.TypeUint64},
			{Name: "end", ComplexType: schema.TypeUint64},
			{Name: "int_attrs", ComplexType: schema.Dict{Key: schema.TypeString, Value: schema.TypeInt64}},
			{Name: "double_attrs", ComplexType: schema.Dict{Key: schema.TypeString, Value: schema.TypeFloat64}},
			{Name: "str_attrs", ComplexType: schema.Dict{Key: schema.TypeString, Value: schema.TypeString}},
			{Name: "bytes_attrs", ComplexType: schema.Dict{Key: schema.TypeString, Value: schema.TypeBytes}},
			{Name: "attrs", ComplexType: schema.Dict{Key: schema.TypeString, Value: schema.TypeBytes}},
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

func otelToYTSpan(span ptrace.Span) (s Span, _ error) {
	getSpanID := func(arr pcommon.SpanID) uint64 {
		return binary.LittleEndian.Uint64(arr[:])
	}

	s = Span{
		TraceID:      span.TraceID().String(),
		SpanID:       getSpanID(span.SpanID()),
		ParentSpanID: nil,
		Name:         span.Name(),
		Kind:         int32(span.Kind()),
		Start:        uint64(span.StartTimestamp()),
		End:          uint64(span.EndTimestamp()),
		IntAttrs:     nil,
		DoubleAttrs:  nil,
		StrAttrs:     nil,
		BytesAttrs:   nil,
		Attrs:        nil,
	}
	if parent := span.ParentSpanID(); !parent.IsEmpty() {
		v := getSpanID(parent)
		s.ParentSpanID = &v
	}

	var rangeErr error
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeInt:
			s.IntAttrs = append(s.IntAttrs, KeyValue[int64]{k, v.Int()})
		case pcommon.ValueTypeDouble:
			s.DoubleAttrs = append(s.DoubleAttrs, KeyValue[float64]{k, v.Double()})
		case pcommon.ValueTypeStr:
			s.StrAttrs = append(s.StrAttrs, KeyValue[string]{k, v.Str()})
		case pcommon.ValueTypeBytes:
			s.BytesAttrs = append(s.BytesAttrs, KeyValue[[]byte]{k, v.Bytes().AsRaw()})
		default:
			data, err := json.Marshal(v.AsRaw())
			if err != nil {
				rangeErr = err
				return false
			}
			s.Attrs = append(s.Attrs, KeyValue[[]byte]{k, data})
		}
		return true
	})
	if rangeErr != nil {
		return s, errors.Wrap(rangeErr, "parse attributes")
	}
	return s, nil
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
				s, err := otelToYTSpan(spans.At(i))
				if err != nil {
					return errors.Wrap(err, "convert span")
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
