// Package tempohandler provides Tempo API implementation.
package tempohandler

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/go-logfmt/logfmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tempoapi.Handler = (*TempoAPI)(nil)

// TempoAPI implements tempoapi.Handler.
type TempoAPI struct {
	q tracestorage.Querier
}

// NewTempoAPI creates new TempoAPI.
func NewTempoAPI(q tracestorage.Querier) *TempoAPI {
	return &TempoAPI{
		q: q,
	}
}

// Echo request for testing, issued by Grafana.
//
// GET /api/echo
func (h *TempoAPI) Echo(_ context.Context) (tempoapi.EchoOK, error) {
	return tempoapi.EchoOK{Data: strings.NewReader("echo")}, nil
}

// Search implements search operation.
// Execute TraceQL query.
//
// GET /api/search
func (h *TempoAPI) Search(ctx context.Context, params tempoapi.SearchParams) (resp *tempoapi.Traces, _ error) {
	var (
		traceQLQuery = params.Q.Value
		logfmtQuery  = params.Tags.Value
	)
	ctx = zctx.With(ctx,
		zap.String("q", traceQLQuery),
		zap.String("tags", logfmtQuery),
	)

	tags, err := parseLogfmt(logfmtQuery)
	if err != nil {
		return nil, errors.Wrap(err, "parse logfmt")
	}

	i, err := h.q.SearchTags(ctx, tags, tracestorage.SearchTagsOptions{
		MinDuration: params.MinDuration.Value,
		MaxDuration: params.MaxDuration.Value,
		Start:       timeToTimestamp(params.Start),
		End:         timeToTimestamp(params.End),
	})
	if err != nil {
		return nil, errors.Wrap(err, "search tags")
	}
	defer func() {
		_ = i.Close()
	}()

	c := metadataCollector{
		limit: params.Limit.Or(20),
	}
	if err := iterators.ForEach(i, c.AddSpan); err != nil {
		return nil, errors.Wrap(err, "map spans")
	}

	return &tempoapi.Traces{
		Traces: c.Result(),
	}, nil
}

func parseLogfmt(q string) (tags map[string]string, _ error) {
	tags = make(map[string]string)
	d := logfmt.NewDecoder(strings.NewReader(q))
	for d.ScanRecord() {
		for d.ScanKeyval() {
			// TODO(tdakkota): bruh allocations
			tags[string(d.Key())] = string(d.Value())
		}
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	return tags, nil
}

// SearchTagValues implements search_tag_values operation.
//
// This endpoint retrieves all discovered values for the given tag, which can be used in search.
//
// GET /api/search/tag/{tag_name}/values
func (h *TempoAPI) SearchTagValues(ctx context.Context, params tempoapi.SearchTagValuesParams) (resp *tempoapi.TagValues, _ error) {
	lg := zctx.From(ctx)

	iter, err := h.q.TagValues(ctx, params.TagName)
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	defer func() {
		_ = iter.Close()
	}()

	var values []string
	if err := iterators.ForEach(iter, func(tag tracestorage.Tag) error {
		values = append(values, tag.Value)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "map tags")
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

	iter, err := h.q.TagValues(ctx, params.TagName)
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	defer func() {
		_ = iter.Close()
	}()

	var values []tempoapi.TagValue
	if err := iterators.ForEach(iter, func(tag tracestorage.Tag) error {
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
			return nil
		}

		values = append(values, tempoapi.TagValue{
			Type:  typ,
			Value: tag.Value,
		})
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "map tags")
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

	names, err := h.q.TagNames(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	lg.Debug("Got tag names", zap.Int("count", len(names)))

	return &tempoapi.TagNames{
		TagNames: names,
	}, nil
}

// TraceByID implements traceByID operation.
//
// Querying traces by id.
//
// GET /api/traces/{traceID}
func (h *TempoAPI) TraceByID(ctx context.Context, params tempoapi.TraceByIDParams) (resp tempoapi.TraceByIDRes, _ error) {
	lg := zctx.From(ctx)

	traceID, err := otelstorage.ParseTraceID(params.TraceID)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid traceID %q", params.TraceID)
	}

	iter, err := h.q.TraceByID(ctx, traceID, tracestorage.TraceByIDOptions{
		Start: timeToTimestamp(params.Start),
		End:   timeToTimestamp(params.End),
	})
	if err != nil {
		return nil, errors.Wrap(err, "query traceID")
	}
	defer func() {
		_ = iter.Close()
	}()

	var c batchCollector
	if err := iterators.ForEach(iter, c.AddSpan); err != nil {
		return nil, errors.Wrap(err, "map spans")
	}

	traces := c.Result()
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

func timeToTimestamp[O interface{ Get() (time.Time, bool) }](o O) otelstorage.Timestamp {
	t, ok := o.Get()
	if !ok {
		return 0
	}
	return otelstorage.NewTimestampFromTime(t)
}
