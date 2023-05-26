package ytstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/go-faster/oteldb/internal/tempoapi"
)

// TempoAPI implements tempoapi.Handler.
type TempoAPI struct {
	yc     yt.Client
	tables tables
}

// NewTempoAPI creates new TempoAPI.
func NewTempoAPI(yc yt.Client, prefix ypath.Path) *TempoAPI {
	return &TempoAPI{
		yc:     yc,
		tables: newTables(prefix),
	}
}

var _ tempoapi.Handler = (*TempoAPI)(nil)

// Search implements search operation.
// Execute TraceQL query.
//
// GET /api/search
func (h *TempoAPI) Search(ctx context.Context, params tempoapi.SearchParams) (*tempoapi.Traces, error) {
	lg := zctx.From(ctx)
	lg.Debug("Search traces",
		zap.String("q", params.Q.Value),
		zap.String("tags", params.Tags.Value),
	)
	return &tempoapi.Traces{}, nil
}

// SearchTagValues implements search_tag_values operation.
//
// This endpoint retrieves all discovered values for the given tag, which can be used in search.
//
// GET /api/search/tag/{tag_name}/values
func (h *TempoAPI) SearchTagValues(ctx context.Context, params tempoapi.SearchTagValuesParams) (resp *tempoapi.TagValues, _ error) {
	lg := zctx.From(ctx)

	query := fmt.Sprintf("value from [%s] where name = %q", h.tables.tags, params.TagName)
	r, err := h.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return resp, err
	}
	defer func() {
		_ = r.Close()
	}()

	var values []string
	for r.Next() {
		var tag Tag
		if err := r.Scan(&tag); err != nil {
			return resp, err
		}
		values = append(values, tag.Value)
	}
	if err := r.Err(); err != nil {
		return resp, err
	}
	lg.Debug("Got tag values",
		zap.String("tag_name", params.TagName),
		zap.Int("count", len(values)),
	)

	return &tempoapi.TagValues{
		TagValues: values,
	}, nil
}

// SearchTagValuesV2 implements search_tag_values_v2 operation.
//
// This endpoint retrieves all discovered values and their data types for the given TraceQL
// identifier.
//
// GET /api/v2/search/tag/{tag_name}/values
func (h *TempoAPI) SearchTagValuesV2(ctx context.Context, params tempoapi.SearchTagValuesV2Params) (resp *tempoapi.TagValuesV2, _ error) {
	lg := zctx.From(ctx)

	query := fmt.Sprintf("type, value from [%s] where name = %q", h.tables.tags, params.TagName)
	r, err := h.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return resp, err
	}
	defer func() {
		_ = r.Close()
	}()

	var values []tempoapi.TagValue
	for r.Next() {
		var tag Tag
		if err := r.Scan(&tag); err != nil {
			return resp, err
		}

		// TODO(tdakkota): handle duration/status and things
		// https://github.com/grafana/tempo/blob/991d72281e5168080f426b3f1c9d5c4b88f7c460/modules/ingester/instance_search.go#L379
		var typ string
		switch pcommon.ValueType(tag.Type) {
		case pcommon.ValueTypeStr:
			typ = "string"
		case pcommon.ValueTypeInt:
			typ = "int"
		case pcommon.ValueTypeDouble:
			typ = "float"
		case pcommon.ValueTypeBool:
			typ = "bool"
		case pcommon.ValueTypeBytes:
			typ = "string"
		case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
			// what?
		}

		values = append(values, tempoapi.TagValue{
			Type:  typ,
			Value: tag.Value,
		})
	}
	if err := r.Err(); err != nil {
		return resp, err
	}
	lg.Debug("Got tag types and values",
		zap.String("tag_name", params.TagName),
		zap.Int("count", len(values)),
	)

	return &tempoapi.TagValuesV2{
		TagValues: values,
	}, nil
}

// SearchTags implements search_tags operation.
//
// This endpoint retrieves all discovered tag names that can be used in search.
//
// GET /api/search/tags
func (h *TempoAPI) SearchTags(ctx context.Context) (resp *tempoapi.TagNames, _ error) {
	lg := zctx.From(ctx)

	query := fmt.Sprintf("name from [%s]", h.tables.tags)
	r, err := h.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return resp, err
	}
	defer func() {
		_ = r.Close()
	}()

	var names []string
	for r.Next() {
		var tag Tag
		if err := r.Scan(&tag); err != nil {
			return resp, err
		}
		names = append(names, tag.Name)
	}
	if err := r.Err(); err != nil {
		return resp, err
	}
	lg.Debug("Got tag names", zap.Int("count", len(names)))

	return &tempoapi.TagNames{
		TagNames: names,
	}, nil
}

func ytToOTELSpan(span Span, s ptrace.Span) {
	getSpanID := func(v uint64) (spanID [8]byte) {
		binary.LittleEndian.PutUint64(spanID[:], v)
		return spanID
	}

	// FIXME(tdakkota): probably, we can just implement YSON (en/de)coder for UUID.
	s.SetTraceID(pcommon.TraceID(span.TraceID))
	s.SetSpanID(pcommon.SpanID(span.SpanID))
	s.TraceState().FromRaw(span.TraceState)
	if p := span.ParentSpanID; p != nil {
		s.SetParentSpanID(getSpanID(*p))
	}
	s.SetName(span.Name)
	s.SetKind(ptrace.SpanKind(span.Kind))
	s.SetStartTimestamp(pcommon.Timestamp(span.Start))
	s.SetEndTimestamp(pcommon.Timestamp(span.End))
	span.Attrs.CopyTo(s.Attributes())

	status := s.Status()
	status.SetCode(ptrace.StatusCode(span.StatusCode))
	status.SetMessage(span.StatusMessage)
}

// TraceByID implements traceByID operation.
//
// Querying traces by id.
//
// GET /api/traces/{traceID}
func (h *TempoAPI) TraceByID(ctx context.Context, params tempoapi.TraceByIDParams) (resp tempoapi.TraceByIDRes, _ error) {
	lg := zctx.From(ctx)
	var (
		start = zap.Skip()
		end   = zap.Skip()

		query = fmt.Sprintf("* from [%s] where trace_id = %q", h.tables.spans, params.TraceID[:])
	)

	if s, ok := params.Start.Get(); ok {
		n := s.UnixNano()
		query += fmt.Sprintf(" and start >= %d", n)
		start = zap.Int64("start", n)
	}
	if s, ok := params.End.Get(); ok {
		n := s.UnixNano()
		query += fmt.Sprintf(" and end <= %d", n)
		end = zap.Int64("end", n)
	}
	lg = lg.With(
		zap.Stringer("look_for", params.TraceID),
		start,
		end,
	)

	r, err := h.yc.SelectRows(ctx, query, nil)
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
		s.ResourceAttrs.CopyTo(res.Attributes())

		scopeSpan := resSpan.ScopeSpans().AppendEmpty()
		scope := scopeSpan.Scope()
		scope.SetName(s.ScopeName)
		scope.SetVersion(s.ScopeVersion)
		s.ScopeAttrs.CopyTo(scope.Attributes())

		ss = scopeSpan.Spans()
		scopeSpans[k] = ss
		return ss
	}

	for r.Next() {
		var span Span
		if err := r.Scan(&span); err != nil {
			return resp, err
		}
		s := getSpanSlice(span).AppendEmpty()
		ytToOTELSpan(span, s)
	}
	if err := r.Err(); err != nil {
		return resp, err
	}

	spanCount := traces.SpanCount()
	lg.Debug("Got trace by ID", zap.Int("span_count", spanCount))
	if spanCount < 1 {
		return &tempoapi.TraceByIDNotFound{}, nil
	}

	m := ptrace.ProtoMarshaler{}
	data, err := m.MarshalTraces(traces)
	if err != nil {
		return resp, errors.Wrap(err, "marshal traces")
	}
	return &tempoapi.TraceByID{Data: bytes.NewReader(data)}, nil
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
