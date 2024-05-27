// Package tempohandler provides Tempo API implementation.
package tempohandler

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/go-logfmt/logfmt"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

// TempoAPI implements tempoapi.Handler.
type TempoAPI struct {
	q      tracestorage.Querier
	engine *traceqlengine.Engine

	enableAutocomplete bool
}

var _ tempoapi.Handler = (*TempoAPI)(nil)

// NewTempoAPI creates new TempoAPI.
func NewTempoAPI(
	q tracestorage.Querier,
	engine *traceqlengine.Engine,
	opts TempoAPIOptions,
) *TempoAPI {
	opts.setDefaults()

	return &TempoAPI{
		q:                  q,
		engine:             engine,
		enableAutocomplete: opts.EnableAutocompleteQuery,
	}
}

// BuildInfo implements buildInfo operation.
//
// Returns Tempo buildinfo, in the same format as Prometheus `/api/v1/status/buildinfo`.
// Used by Grafana to check Tempo API version.
//
// GET /api/status/buildinfo
func (h *TempoAPI) BuildInfo(ctx context.Context) (*tempoapi.PrometheusVersion, error) {
	// defaultTempoVersion is a default Tempo version used by Grafana, in
	// case if buildinfo request fails.
	const defaultTempoVersion = "2.1.0"
	return &tempoapi.PrometheusVersion{
		Version:   defaultTempoVersion,
		GoVersion: runtime.Version(),
	}, nil
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

	switch {
	case traceQLQuery != "":
		return h.searchTraceQL(ctx, traceQLQuery, params)
	case logfmtQuery != "":
		return h.searchTags(ctx, logfmtQuery, params)
	default:
		return nil, &tempoapi.ErrorStatusCode{
			StatusCode: http.StatusBadRequest,
			Response:   `either of parameters "q" and "tags" should be set`,
		}
	}
}

func (h *TempoAPI) searchTraceQL(ctx context.Context, query string, params tempoapi.SearchParams) (resp *tempoapi.Traces, _ error) {
	if h.engine == nil {
		return nil, &tempoapi.ErrorStatusCode{
			StatusCode: http.StatusInternalServerError,
			Response:   "TraceQL engine is disabled",
		}
	}
	return h.engine.Eval(ctx, query, traceqlengine.EvalParams{
		MinDuration: params.MinDuration.Or(0),
		MaxDuration: params.MinDuration.Or(0),
		Start:       timeToTimestamp(params.Start),
		End:         timeToTimestamp(params.End),
		Limit:       params.Limit.Or(20),
	})
}

func (h *TempoAPI) searchTags(ctx context.Context, query string, params tempoapi.SearchParams) (resp *tempoapi.Traces, _ error) {
	tags, err := parseLogfmt(query)
	if err != nil {
		return nil, &tempoapi.ErrorStatusCode{
			StatusCode: http.StatusBadRequest,
			Response:   tempoapi.Error(fmt.Sprintf("parse logfmt: %s", err)),
		}
	}

	i, err := h.q.SearchTags(ctx, tags, tracestorage.SearchTagsOptions{
		MinDuration: params.MinDuration.Or(0),
		MaxDuration: params.MaxDuration.Or(0),
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

	var (
		attr  = traceql.Attribute{Name: params.TagName}
		query traceql.Autocomplete
	)
	if q, ok := params.Q.Get(); ok && h.enableAutocomplete {
		query = traceql.ParseAutocomplete(q)
	}

	iter, err := h.q.TagValues(ctx, attr, tracestorage.TagValuesOptions{
		Query: query,
		Start: timeToTimestamp(params.Start),
		End:   timeToTimestamp(params.End),
	})
	if err != nil {
		return nil, errors.Wrap(err, "get tag values")
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
		zap.String("q", params.Q.Or("")),
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
// GET /api/v2/search/tag/{attribute_selector}/values
func (h *TempoAPI) SearchTagValuesV2(ctx context.Context, params tempoapi.SearchTagValuesV2Params) (resp *tempoapi.TagValuesV2, _ error) {
	lg := zctx.From(ctx)

	attr, err := traceql.ParseAttribute(params.AttributeSelector)
	if err != nil {
		return nil, err
	}
	var query traceql.Autocomplete
	if q, ok := params.Q.Get(); ok && h.enableAutocomplete {
		query = traceql.ParseAutocomplete(q)
	}

	iter, err := h.q.TagValues(ctx, attr, tracestorage.TagValuesOptions{
		Query: query,
		Start: timeToTimestamp(params.Start),
		End:   timeToTimestamp(params.End),
	})
	if err != nil {
		return nil, errors.Wrap(err, "get tag values")
	}
	defer func() {
		_ = iter.Close()
	}()

	var (
		values   []tempoapi.TagValue
		typeWarn bool
	)
	if err := iterators.ForEach(iter, func(tag tracestorage.Tag) error {
		var typ string
		switch tag.Type {
		case traceql.TypeString:
			typ = "string"
		case traceql.TypeInt:
			typ = "int"
		case traceql.TypeNumber:
			typ = "float"
		case traceql.TypeBool:
			typ = "bool"
		case traceql.TypeSpanKind, traceql.TypeSpanStatus:
			typ = "keyword"
		case traceql.TypeDuration:
			typ = "duration"
		default:
			if !typeWarn {
				// Warn only once.
				typeWarn = true
				lg.Warn("Unexpected tag type",
					zap.Stringer("type", tag.Type),
					zap.String("tag", tag.Name),
				)
			}
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
	lg.Debug("Got tag values",
		zap.String("attribute_selector", params.AttributeSelector),
		zap.String("q", params.Q.Or("")),
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
func (h *TempoAPI) SearchTags(ctx context.Context, params tempoapi.SearchTagsParams) (resp *tempoapi.TagNames, _ error) {
	lg := zctx.From(ctx)

	var scope traceql.AttributeScope
	switch params.Scope.Or(tempoapi.TagScopeNone) {
	case tempoapi.TagScopeSpan:
		scope = traceql.ScopeSpan
	case tempoapi.TagScopeResource:
		scope = traceql.ScopeResource
	case tempoapi.TagScopeIntrinsic:
		lg.Debug("Return intrinsic names")
		return &tempoapi.TagNames{
			TagNames: traceql.IntrinsicNames(),
		}, nil
	case tempoapi.TagScopeNone:
		scope = traceql.ScopeNone
	}

	tags, err := h.q.TagNames(ctx, tracestorage.TagNamesOptions{
		Scope: scope,
		Start: timeToTimestamp(params.Start),
		End:   timeToTimestamp(params.End),
	})
	if err != nil {
		return nil, errors.Wrap(err, "get tag names")
	}

	names := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		names[tag.Name] = struct{}{}
	}
	lg.Debug("Got tag names", zap.Int("count", len(names)))

	return &tempoapi.TagNames{
		TagNames: maps.Keys(names),
	}, nil
}

// SearchTagsV2 implements searchTagsV2 operation.
//
// This endpoint retrieves all discovered tag names that can be used in search.
//
// GET /api/v2/search/tags
func (h *TempoAPI) SearchTagsV2(ctx context.Context, params tempoapi.SearchTagsV2Params) (*tempoapi.TagNamesV2, error) {
	lg := zctx.From(ctx)

	var (
		searchScope traceql.AttributeScope
		intrinsic   = tempoapi.ScopeTags{
			Name: tempoapi.TagScopeIntrinsic,
			Tags: traceql.IntrinsicNames(),
		}
	)
	switch params.Scope.Or(tempoapi.TagScopeNone) {
	case tempoapi.TagScopeSpan:
		searchScope = traceql.ScopeSpan
	case tempoapi.TagScopeResource:
		searchScope = traceql.ScopeResource
	case tempoapi.TagScopeIntrinsic:
		lg.Debug("Return intrinsic names")
		return &tempoapi.TagNamesV2{
			Scopes: []tempoapi.ScopeTags{intrinsic},
		}, nil
	case tempoapi.TagScopeNone:
		searchScope = traceql.ScopeNone
	}

	tags, err := h.q.TagNames(ctx, tracestorage.TagNamesOptions{
		Scope: searchScope,
		Start: timeToTimestamp(params.Start),
		End:   timeToTimestamp(params.End),
	})
	if err != nil {
		return nil, errors.Wrap(err, "get tag names")
	}

	scopes := make(map[tempoapi.TagScope]tempoapi.ScopeTags, 4)
	if searchScope == traceql.ScopeNone {
		// Add intrinsics to the result, if all scopes are requested.
		scopes[intrinsic.Name] = intrinsic
	}
	for _, tag := range tags {
		var tagScope tempoapi.TagScope
		switch tag.Scope {
		case traceql.ScopeNone:
			tagScope = tempoapi.TagScopeNone
		case traceql.ScopeResource, traceql.ScopeInstrumentation:
			tagScope = tempoapi.TagScopeResource
		case traceql.ScopeSpan:
			tagScope = tempoapi.TagScopeSpan
		default:
			lg.Warn("Unexpected tag scope",
				zap.Stringer("scope", tag.Scope),
				zap.String("tag", tag.Name),
			)
			continue
		}

		scopeTags, ok := scopes[tagScope]
		if !ok {
			scopeTags.Name = tagScope
		}
		scopeTags.Tags = append(scopeTags.Tags, tag.Name)
		scopes[tagScope] = scopeTags
	}

	return &tempoapi.TagNamesV2{
		Scopes: maps.Values(scopes),
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
