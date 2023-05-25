package ytstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"

	"github.com/google/uuid"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/go-faster/oteldb/internal/tempoapi"
)

// TempoAPI implements tempoapi.Handler.
type TempoAPI struct {
	yc    yt.Client
	table ypath.Path
}

// NewTempoAPI creates new TempoAPI.
func NewTempoAPI(yc yt.Client, table ypath.Path) *TempoAPI {
	return &TempoAPI{
		yc:    yc,
		table: table,
	}
}

var _ tempoapi.Handler = (*TempoAPI)(nil)

// Search implements search operation.
// Execute TraceQL query.
//
// GET /api/search
func (h *TempoAPI) Search(ctx context.Context, params tempoapi.SearchParams) (*tempoapi.Traces, error) {
	lg := zctx.From(ctx)
	lg.Info("Get trace by ID",
		zap.String("q", params.Q.Value),
		zap.String("tags", params.Tags.Value),
	)
	return &tempoapi.Traces{}, nil
}

// SearchTagValues implements search_tag_values operation.
//
// This endpoint retrieves all discovered values for the given tag, which can be used in search.
//
// GET /api/search/tag/{service_name}/values
func (h *TempoAPI) SearchTagValues(context.Context, tempoapi.SearchTagValuesParams) (*tempoapi.TagValues, error) {
	return &tempoapi.TagValues{
		TagValues: []string{},
	}, nil
}

// SearchTagValuesV2 implements search_tag_values_v2 operation.
//
// This endpoint retrieves all discovered values and their data types for the given TraceQL
// identifier.
//
// GET /api/v2/search/tag/{service_name}/values
func (h *TempoAPI) SearchTagValuesV2(context.Context, tempoapi.SearchTagValuesV2Params) (*tempoapi.TagValuesV2, error) {
	return &tempoapi.TagValuesV2{
		TagValues: []tempoapi.TagValue{},
	}, nil
}

// SearchTags implements search_tags operation.
//
// This endpoint retrieves all discovered tag names that can be used in search.
//
// GET /api/search/tags
func (h *TempoAPI) SearchTags(context.Context) (*tempoapi.TagNames, error) {
	return &tempoapi.TagNames{
		TagNames: []string{},
	}, nil
}

type hexUUID uuid.UUID

func (h hexUUID) String() string {
	const hextable = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(32)
	for _, c := range h {
		sb.WriteByte(hextable[c>>4])
		sb.WriteByte(hextable[c&0x0f])
	}
	return sb.String()
}

func ytToOTELSpan(span Span, s ptrace.Span) {
	getSpanID := func(v uint64) (spanID [8]byte) {
		binary.LittleEndian.PutUint64(spanID[:], v)
		return spanID
	}

	// FIXME(tdakkota): probably, we can just implement YSON (en/de)coder for UUID.
	traceID := uuid.MustParse(span.TraceID)
	s.SetTraceID(pcommon.TraceID(traceID))
	s.SetSpanID(getSpanID(span.SpanID))
	s.TraceState().FromRaw(span.TraceState)
	if p := span.ParentSpanID; p != nil {
		s.SetParentSpanID(getSpanID(*p))
	}
	s.SetName(span.Name)
	s.SetKind(ptrace.SpanKind(span.Kind))
	s.SetStartTimestamp(pcommon.Timestamp(span.Start))
	s.SetEndTimestamp(pcommon.Timestamp(span.End))
	pcommon.Map(span.Attrs).CopyTo(s.Attributes())

	status := s.Status()
	status.SetCode(ptrace.StatusCode(span.StatusCode))
	status.SetMessage(span.StatusMessage)
}

// TraceByID implements traceByID operation.
//
// Querying traces by id.
//
// GET /api/traces/{traceID}
func (h *TempoAPI) TraceByID(ctx context.Context, params tempoapi.TraceByIDParams) (resp tempoapi.TraceByID, _ error) {
	lg := zctx.From(ctx)

	r, err := h.yc.SelectRows(ctx, fmt.Sprintf("* from [%s] where trace_id = %q", h.table, hexUUID(params.TraceID)), nil)
	if err != nil {
		return resp, err
	}
	defer func() {
		_ = r.Close()
	}()

	type spanKey struct {
		batchID      string
		scopeName    string
		scopeVersion string
	}

	traces := ptrace.NewTraces()
	resSpans := map[string]ptrace.ResourceSpans{}
	scopeSpans := map[spanKey]ptrace.SpanSlice{}
	getSpanSlice := func(s Span) ptrace.SpanSlice {
		k := spanKey{
			batchID:      s.BatchID,
			scopeName:    s.ScopeName,
			scopeVersion: s.ScopeVersion,
		}

		ss, ok := scopeSpans[k]
		if ok {
			return ss
		}

		resSpan, ok := resSpans[s.BatchID]
		if !ok {
			resSpan = traces.ResourceSpans().AppendEmpty()
			resSpans[s.BatchID] = resSpan
		}
		res := resSpan.Resource()
		pcommon.Map(s.ResourceAttrs).CopyTo(res.Attributes())

		scopeSpan := resSpan.ScopeSpans().AppendEmpty()
		scope := scopeSpan.Scope()
		scope.SetName(s.ScopeName)
		scope.SetVersion(s.ScopeVersion)
		pcommon.Map(s.ScopeAttrs).CopyTo(scope.Attributes())

		ss = scopeSpan.Spans()
		scopeSpans[k] = ss
		return ss
	}

	n := 0
	for r.Next() {
		var span Span
		if err := r.Scan(&span); err != nil {
			return resp, err
		}
		s := getSpanSlice(span).AppendEmpty()
		ytToOTELSpan(span, s)
		n++
	}
	if err := r.Err(); err != nil {
		return resp, err
	}
	lg.Info("Get trace by ID", zap.Stringer("look_for", params.TraceID),
		zap.Int("got", n),
		zap.Int("span_count", traces.SpanCount()),
	)

	m := ptrace.ProtoMarshaler{}
	data, err := m.MarshalTraces(traces)
	if err != nil {
		return resp, errors.Wrap(err, "marshal traces")
	}
	return tempoapi.TraceByID{Data: bytes.NewReader(data)}, nil
}

// NewError creates *ErrorStatusCode from error returned by handler.
//
// Used for common default response.
func (h *TempoAPI) NewError(_ context.Context, err error) *tempoapi.ErrorStatusCode {
	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   tempoapi.Error(err.Error()),
	}
}
