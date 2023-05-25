// Package ytstore provide YTSaurus-based trace storage.
package ytstore

import (
	"context"
	"encoding/binary"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/otelreceiver"
)

var _ otelreceiver.Handler = (*Store)(nil)

// Store implements ytsaurus-based trace storage.
type Store struct {
	yc     yt.Client
	tables tables
}

// NewStore creates new Store.
func NewStore(yc yt.Client, prefix ypath.Path) *Store {
	return &Store{
		yc:     yc,
		tables: newTables(prefix),
	}
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
	tags := map[Tag]struct{}{}
	addTags := func(attrs pcommon.Map) {
		attrs.Range(func(k string, v pcommon.Value) bool {
			switch t := v.Type(); t {
			case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
			default:
				tags[Tag{k, v.AsString(), int32(t)}] = struct{}{}
			}
			return true
		})
	}

	bw := s.yc.NewRowBatchWriter()
	resSpans := traces.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		batchID := uuid.New().String()
		resSpan := resSpans.At(i)
		res := resSpan.Resource()
		addTags(res.Attributes())

		scopeSpans := resSpan.ScopeSpans()
		for i := 0; i < scopeSpans.Len(); i++ {
			scopeSpan := scopeSpans.At(i)
			scope := scopeSpan.Scope()
			addTags(scope.Attributes())

			spans := scopeSpan.Spans()
			for i := 0; i < spans.Len(); i++ {
				span := spans.At(i)

				s := otelToYTSpan(batchID, res, scope, span)
				if err := bw.Write(s); err != nil {
					return errors.Wrap(err, "write span")
				}
				addTags(span.Attributes())
			}
		}
	}

	if err := bw.Commit(); err != nil {
		return errors.Wrap(err, "commit")
	}
	if err := s.yc.InsertRowBatch(ctx, s.tables.spans, bw.Batch(), &yt.InsertRowsOptions{}); err != nil {
		return errors.Wrap(err, "insert spans")
	}

	bw = s.yc.NewRowBatchWriter()
	for k := range tags {
		if err := bw.Write(k); err != nil {
			return errors.Wrap(err, "write tag")
		}
	}
	if err := bw.Commit(); err != nil {
		return errors.Wrap(err, "commit")
	}
	if err := s.yc.InsertRowBatch(ctx, s.tables.tags, bw.Batch(), &yt.InsertRowsOptions{}); err != nil {
		return errors.Wrap(err, "insert spans")
	}
	return nil
}
