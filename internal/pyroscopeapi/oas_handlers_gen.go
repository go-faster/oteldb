// Code generated by ogen, DO NOT EDIT.

package pyroscopeapi

import (
	"context"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	ht "github.com/ogen-go/ogen/http"
	"github.com/ogen-go/ogen/middleware"
	"github.com/ogen-go/ogen/ogenerrors"
	"github.com/ogen-go/ogen/otelogen"
)

type codeRecorder struct {
	http.ResponseWriter
	status int
}

func (c *codeRecorder) WriteHeader(status int) {
	c.status = status
	c.ResponseWriter.WriteHeader(status)
}

// handleGetAppsRequest handles getApps operation.
//
// Returns list of application metadata.
// Used by Grafana to test connection to Pyroscope.
//
// GET /api/apps
func (s *Server) handleGetAppsRequest(args [0]string, argsEscaped bool, w http.ResponseWriter, r *http.Request) {
	statusWriter := &codeRecorder{ResponseWriter: w}
	w = statusWriter
	otelAttrs := []attribute.KeyValue{
		otelogen.OperationID("getApps"),
		semconv.HTTPRequestMethodKey.String("GET"),
		semconv.HTTPRouteKey.String("/api/apps"),
	}

	// Start a span for this request.
	ctx, span := s.cfg.Tracer.Start(r.Context(), GetAppsOperation,
		trace.WithAttributes(otelAttrs...),
		serverSpanKind,
	)
	defer span.End()

	// Add Labeler to context.
	labeler := &Labeler{attrs: otelAttrs}
	ctx = contextWithLabeler(ctx, labeler)

	// Run stopwatch.
	startTime := time.Now()
	defer func() {
		elapsedDuration := time.Since(startTime)

		attrSet := labeler.AttributeSet()
		attrs := attrSet.ToSlice()
		code := statusWriter.status
		if code != 0 {
			codeAttr := semconv.HTTPResponseStatusCode(code)
			attrs = append(attrs, codeAttr)
			span.SetAttributes(codeAttr)
		}
		attrOpt := metric.WithAttributes(attrs...)

		// Increment request counter.
		s.requests.Add(ctx, 1, attrOpt)

		// Use floating point division here for higher precision (instead of Millisecond method).
		s.duration.Record(ctx, float64(elapsedDuration)/float64(time.Millisecond), attrOpt)
	}()

	var (
		recordError = func(stage string, err error) {
			span.RecordError(err)

			// https://opentelemetry.io/docs/specs/semconv/http/http-spans/#status
			// Span Status MUST be left unset if HTTP status code was in the 1xx, 2xx or 3xx ranges,
			// unless there was another error (e.g., network error receiving the response body; or 3xx codes with
			// max redirects exceeded), in which case status MUST be set to Error.
			code := statusWriter.status
			if code >= 100 && code < 500 {
				span.SetStatus(codes.Error, stage)
			}

			attrSet := labeler.AttributeSet()
			attrs := attrSet.ToSlice()
			if code != 0 {
				attrs = append(attrs, semconv.HTTPResponseStatusCode(code))
			}

			s.errors.Add(ctx, 1, metric.WithAttributes(attrs...))
		}
		err error
	)

	var response []ApplicationMetadata
	if m := s.cfg.Middleware; m != nil {
		mreq := middleware.Request{
			Context:          ctx,
			OperationName:    GetAppsOperation,
			OperationSummary: "",
			OperationID:      "getApps",
			Body:             nil,
			Params:           middleware.Parameters{},
			Raw:              r,
		}

		type (
			Request  = struct{}
			Params   = struct{}
			Response = []ApplicationMetadata
		)
		response, err = middleware.HookMiddleware[
			Request,
			Params,
			Response,
		](
			m,
			mreq,
			nil,
			func(ctx context.Context, request Request, params Params) (response Response, err error) {
				response, err = s.h.GetApps(ctx)
				return response, err
			},
		)
	} else {
		response, err = s.h.GetApps(ctx)
	}
	if err != nil {
		if errRes, ok := errors.Into[*ErrorStatusCode](err); ok {
			if err := encodeErrorResponse(errRes, w, span); err != nil {
				defer recordError("Internal", err)
			}
			return
		}
		if errors.Is(err, ht.ErrNotImplemented) {
			s.cfg.ErrorHandler(ctx, w, r, err)
			return
		}
		if err := encodeErrorResponse(s.h.NewError(ctx, err), w, span); err != nil {
			defer recordError("Internal", err)
		}
		return
	}

	if err := encodeGetAppsResponse(response, w, span); err != nil {
		defer recordError("EncodeResponse", err)
		if !errors.Is(err, ht.ErrInternalServerErrorResponse) {
			s.cfg.ErrorHandler(ctx, w, r, err)
		}
		return
	}
}

// handleIngestRequest handles ingest operation.
//
// Push data to Pyroscope.
//
// POST /ingest
func (s *Server) handleIngestRequest(args [0]string, argsEscaped bool, w http.ResponseWriter, r *http.Request) {
	statusWriter := &codeRecorder{ResponseWriter: w}
	w = statusWriter
	otelAttrs := []attribute.KeyValue{
		otelogen.OperationID("ingest"),
		semconv.HTTPRequestMethodKey.String("POST"),
		semconv.HTTPRouteKey.String("/ingest"),
	}

	// Start a span for this request.
	ctx, span := s.cfg.Tracer.Start(r.Context(), IngestOperation,
		trace.WithAttributes(otelAttrs...),
		serverSpanKind,
	)
	defer span.End()

	// Add Labeler to context.
	labeler := &Labeler{attrs: otelAttrs}
	ctx = contextWithLabeler(ctx, labeler)

	// Run stopwatch.
	startTime := time.Now()
	defer func() {
		elapsedDuration := time.Since(startTime)

		attrSet := labeler.AttributeSet()
		attrs := attrSet.ToSlice()
		code := statusWriter.status
		if code != 0 {
			codeAttr := semconv.HTTPResponseStatusCode(code)
			attrs = append(attrs, codeAttr)
			span.SetAttributes(codeAttr)
		}
		attrOpt := metric.WithAttributes(attrs...)

		// Increment request counter.
		s.requests.Add(ctx, 1, attrOpt)

		// Use floating point division here for higher precision (instead of Millisecond method).
		s.duration.Record(ctx, float64(elapsedDuration)/float64(time.Millisecond), attrOpt)
	}()

	var (
		recordError = func(stage string, err error) {
			span.RecordError(err)

			// https://opentelemetry.io/docs/specs/semconv/http/http-spans/#status
			// Span Status MUST be left unset if HTTP status code was in the 1xx, 2xx or 3xx ranges,
			// unless there was another error (e.g., network error receiving the response body; or 3xx codes with
			// max redirects exceeded), in which case status MUST be set to Error.
			code := statusWriter.status
			if code >= 100 && code < 500 {
				span.SetStatus(codes.Error, stage)
			}

			attrSet := labeler.AttributeSet()
			attrs := attrSet.ToSlice()
			if code != 0 {
				attrs = append(attrs, semconv.HTTPResponseStatusCode(code))
			}

			s.errors.Add(ctx, 1, metric.WithAttributes(attrs...))
		}
		err          error
		opErrContext = ogenerrors.OperationContext{
			Name: IngestOperation,
			ID:   "ingest",
		}
	)
	params, err := decodeIngestParams(args, argsEscaped, r)
	if err != nil {
		err = &ogenerrors.DecodeParamsError{
			OperationContext: opErrContext,
			Err:              err,
		}
		defer recordError("DecodeParams", err)
		s.cfg.ErrorHandler(ctx, w, r, err)
		return
	}
	request, close, err := s.decodeIngestRequest(r)
	if err != nil {
		err = &ogenerrors.DecodeRequestError{
			OperationContext: opErrContext,
			Err:              err,
		}
		defer recordError("DecodeRequest", err)
		s.cfg.ErrorHandler(ctx, w, r, err)
		return
	}
	defer func() {
		if err := close(); err != nil {
			recordError("CloseRequest", err)
		}
	}()

	var response *IngestOK
	if m := s.cfg.Middleware; m != nil {
		mreq := middleware.Request{
			Context:          ctx,
			OperationName:    IngestOperation,
			OperationSummary: "",
			OperationID:      "ingest",
			Body:             request,
			Params: middleware.Parameters{
				{
					Name: "name",
					In:   "query",
				}: params.Name,
				{
					Name: "from",
					In:   "query",
				}: params.From,
				{
					Name: "until",
					In:   "query",
				}: params.Until,
				{
					Name: "sampleRate",
					In:   "query",
				}: params.SampleRate,
				{
					Name: "spyName",
					In:   "query",
				}: params.SpyName,
				{
					Name: "units",
					In:   "query",
				}: params.Units,
				{
					Name: "aggregationType",
					In:   "query",
				}: params.AggregationType,
				{
					Name: "format",
					In:   "query",
				}: params.Format,
			},
			Raw: r,
		}

		type (
			Request  = *IngestReqWithContentType
			Params   = IngestParams
			Response = *IngestOK
		)
		response, err = middleware.HookMiddleware[
			Request,
			Params,
			Response,
		](
			m,
			mreq,
			unpackIngestParams,
			func(ctx context.Context, request Request, params Params) (response Response, err error) {
				err = s.h.Ingest(ctx, request, params)
				return response, err
			},
		)
	} else {
		err = s.h.Ingest(ctx, request, params)
	}
	if err != nil {
		if errRes, ok := errors.Into[*ErrorStatusCode](err); ok {
			if err := encodeErrorResponse(errRes, w, span); err != nil {
				defer recordError("Internal", err)
			}
			return
		}
		if errors.Is(err, ht.ErrNotImplemented) {
			s.cfg.ErrorHandler(ctx, w, r, err)
			return
		}
		if err := encodeErrorResponse(s.h.NewError(ctx, err), w, span); err != nil {
			defer recordError("Internal", err)
		}
		return
	}

	if err := encodeIngestResponse(response, w, span); err != nil {
		defer recordError("EncodeResponse", err)
		if !errors.Is(err, ht.ErrInternalServerErrorResponse) {
			s.cfg.ErrorHandler(ctx, w, r, err)
		}
		return
	}
}

// handleLabelValuesRequest handles labelValues operation.
//
// Returns list of label values.
//
// GET /label-values
func (s *Server) handleLabelValuesRequest(args [0]string, argsEscaped bool, w http.ResponseWriter, r *http.Request) {
	statusWriter := &codeRecorder{ResponseWriter: w}
	w = statusWriter
	otelAttrs := []attribute.KeyValue{
		otelogen.OperationID("labelValues"),
		semconv.HTTPRequestMethodKey.String("GET"),
		semconv.HTTPRouteKey.String("/label-values"),
	}

	// Start a span for this request.
	ctx, span := s.cfg.Tracer.Start(r.Context(), LabelValuesOperation,
		trace.WithAttributes(otelAttrs...),
		serverSpanKind,
	)
	defer span.End()

	// Add Labeler to context.
	labeler := &Labeler{attrs: otelAttrs}
	ctx = contextWithLabeler(ctx, labeler)

	// Run stopwatch.
	startTime := time.Now()
	defer func() {
		elapsedDuration := time.Since(startTime)

		attrSet := labeler.AttributeSet()
		attrs := attrSet.ToSlice()
		code := statusWriter.status
		if code != 0 {
			codeAttr := semconv.HTTPResponseStatusCode(code)
			attrs = append(attrs, codeAttr)
			span.SetAttributes(codeAttr)
		}
		attrOpt := metric.WithAttributes(attrs...)

		// Increment request counter.
		s.requests.Add(ctx, 1, attrOpt)

		// Use floating point division here for higher precision (instead of Millisecond method).
		s.duration.Record(ctx, float64(elapsedDuration)/float64(time.Millisecond), attrOpt)
	}()

	var (
		recordError = func(stage string, err error) {
			span.RecordError(err)

			// https://opentelemetry.io/docs/specs/semconv/http/http-spans/#status
			// Span Status MUST be left unset if HTTP status code was in the 1xx, 2xx or 3xx ranges,
			// unless there was another error (e.g., network error receiving the response body; or 3xx codes with
			// max redirects exceeded), in which case status MUST be set to Error.
			code := statusWriter.status
			if code >= 100 && code < 500 {
				span.SetStatus(codes.Error, stage)
			}

			attrSet := labeler.AttributeSet()
			attrs := attrSet.ToSlice()
			if code != 0 {
				attrs = append(attrs, semconv.HTTPResponseStatusCode(code))
			}

			s.errors.Add(ctx, 1, metric.WithAttributes(attrs...))
		}
		err          error
		opErrContext = ogenerrors.OperationContext{
			Name: LabelValuesOperation,
			ID:   "labelValues",
		}
	)
	params, err := decodeLabelValuesParams(args, argsEscaped, r)
	if err != nil {
		err = &ogenerrors.DecodeParamsError{
			OperationContext: opErrContext,
			Err:              err,
		}
		defer recordError("DecodeParams", err)
		s.cfg.ErrorHandler(ctx, w, r, err)
		return
	}

	var response LabelValues
	if m := s.cfg.Middleware; m != nil {
		mreq := middleware.Request{
			Context:          ctx,
			OperationName:    LabelValuesOperation,
			OperationSummary: "",
			OperationID:      "labelValues",
			Body:             nil,
			Params: middleware.Parameters{
				{
					Name: "label",
					In:   "query",
				}: params.Label,
				{
					Name: "from",
					In:   "query",
				}: params.From,
				{
					Name: "until",
					In:   "query",
				}: params.Until,
				{
					Name: "query",
					In:   "query",
				}: params.Query,
			},
			Raw: r,
		}

		type (
			Request  = struct{}
			Params   = LabelValuesParams
			Response = LabelValues
		)
		response, err = middleware.HookMiddleware[
			Request,
			Params,
			Response,
		](
			m,
			mreq,
			unpackLabelValuesParams,
			func(ctx context.Context, request Request, params Params) (response Response, err error) {
				response, err = s.h.LabelValues(ctx, params)
				return response, err
			},
		)
	} else {
		response, err = s.h.LabelValues(ctx, params)
	}
	if err != nil {
		if errRes, ok := errors.Into[*ErrorStatusCode](err); ok {
			if err := encodeErrorResponse(errRes, w, span); err != nil {
				defer recordError("Internal", err)
			}
			return
		}
		if errors.Is(err, ht.ErrNotImplemented) {
			s.cfg.ErrorHandler(ctx, w, r, err)
			return
		}
		if err := encodeErrorResponse(s.h.NewError(ctx, err), w, span); err != nil {
			defer recordError("Internal", err)
		}
		return
	}

	if err := encodeLabelValuesResponse(response, w, span); err != nil {
		defer recordError("EncodeResponse", err)
		if !errors.Is(err, ht.ErrInternalServerErrorResponse) {
			s.cfg.ErrorHandler(ctx, w, r, err)
		}
		return
	}
}

// handleLabelsRequest handles labels operation.
//
// Returns list of labels.
//
// GET /labels
func (s *Server) handleLabelsRequest(args [0]string, argsEscaped bool, w http.ResponseWriter, r *http.Request) {
	statusWriter := &codeRecorder{ResponseWriter: w}
	w = statusWriter
	otelAttrs := []attribute.KeyValue{
		otelogen.OperationID("labels"),
		semconv.HTTPRequestMethodKey.String("GET"),
		semconv.HTTPRouteKey.String("/labels"),
	}

	// Start a span for this request.
	ctx, span := s.cfg.Tracer.Start(r.Context(), LabelsOperation,
		trace.WithAttributes(otelAttrs...),
		serverSpanKind,
	)
	defer span.End()

	// Add Labeler to context.
	labeler := &Labeler{attrs: otelAttrs}
	ctx = contextWithLabeler(ctx, labeler)

	// Run stopwatch.
	startTime := time.Now()
	defer func() {
		elapsedDuration := time.Since(startTime)

		attrSet := labeler.AttributeSet()
		attrs := attrSet.ToSlice()
		code := statusWriter.status
		if code != 0 {
			codeAttr := semconv.HTTPResponseStatusCode(code)
			attrs = append(attrs, codeAttr)
			span.SetAttributes(codeAttr)
		}
		attrOpt := metric.WithAttributes(attrs...)

		// Increment request counter.
		s.requests.Add(ctx, 1, attrOpt)

		// Use floating point division here for higher precision (instead of Millisecond method).
		s.duration.Record(ctx, float64(elapsedDuration)/float64(time.Millisecond), attrOpt)
	}()

	var (
		recordError = func(stage string, err error) {
			span.RecordError(err)

			// https://opentelemetry.io/docs/specs/semconv/http/http-spans/#status
			// Span Status MUST be left unset if HTTP status code was in the 1xx, 2xx or 3xx ranges,
			// unless there was another error (e.g., network error receiving the response body; or 3xx codes with
			// max redirects exceeded), in which case status MUST be set to Error.
			code := statusWriter.status
			if code >= 100 && code < 500 {
				span.SetStatus(codes.Error, stage)
			}

			attrSet := labeler.AttributeSet()
			attrs := attrSet.ToSlice()
			if code != 0 {
				attrs = append(attrs, semconv.HTTPResponseStatusCode(code))
			}

			s.errors.Add(ctx, 1, metric.WithAttributes(attrs...))
		}
		err          error
		opErrContext = ogenerrors.OperationContext{
			Name: LabelsOperation,
			ID:   "labels",
		}
	)
	params, err := decodeLabelsParams(args, argsEscaped, r)
	if err != nil {
		err = &ogenerrors.DecodeParamsError{
			OperationContext: opErrContext,
			Err:              err,
		}
		defer recordError("DecodeParams", err)
		s.cfg.ErrorHandler(ctx, w, r, err)
		return
	}

	var response Labels
	if m := s.cfg.Middleware; m != nil {
		mreq := middleware.Request{
			Context:          ctx,
			OperationName:    LabelsOperation,
			OperationSummary: "",
			OperationID:      "labels",
			Body:             nil,
			Params: middleware.Parameters{
				{
					Name: "from",
					In:   "query",
				}: params.From,
				{
					Name: "until",
					In:   "query",
				}: params.Until,
				{
					Name: "query",
					In:   "query",
				}: params.Query,
			},
			Raw: r,
		}

		type (
			Request  = struct{}
			Params   = LabelsParams
			Response = Labels
		)
		response, err = middleware.HookMiddleware[
			Request,
			Params,
			Response,
		](
			m,
			mreq,
			unpackLabelsParams,
			func(ctx context.Context, request Request, params Params) (response Response, err error) {
				response, err = s.h.Labels(ctx, params)
				return response, err
			},
		)
	} else {
		response, err = s.h.Labels(ctx, params)
	}
	if err != nil {
		if errRes, ok := errors.Into[*ErrorStatusCode](err); ok {
			if err := encodeErrorResponse(errRes, w, span); err != nil {
				defer recordError("Internal", err)
			}
			return
		}
		if errors.Is(err, ht.ErrNotImplemented) {
			s.cfg.ErrorHandler(ctx, w, r, err)
			return
		}
		if err := encodeErrorResponse(s.h.NewError(ctx, err), w, span); err != nil {
			defer recordError("Internal", err)
		}
		return
	}

	if err := encodeLabelsResponse(response, w, span); err != nil {
		defer recordError("EncodeResponse", err)
		if !errors.Is(err, ht.ErrInternalServerErrorResponse) {
			s.cfg.ErrorHandler(ctx, w, r, err)
		}
		return
	}
}

// handleRenderRequest handles render operation.
//
// Renders given query.
// One of `query` or `key` is required.
//
// GET /render
func (s *Server) handleRenderRequest(args [0]string, argsEscaped bool, w http.ResponseWriter, r *http.Request) {
	statusWriter := &codeRecorder{ResponseWriter: w}
	w = statusWriter
	otelAttrs := []attribute.KeyValue{
		otelogen.OperationID("render"),
		semconv.HTTPRequestMethodKey.String("GET"),
		semconv.HTTPRouteKey.String("/render"),
	}

	// Start a span for this request.
	ctx, span := s.cfg.Tracer.Start(r.Context(), RenderOperation,
		trace.WithAttributes(otelAttrs...),
		serverSpanKind,
	)
	defer span.End()

	// Add Labeler to context.
	labeler := &Labeler{attrs: otelAttrs}
	ctx = contextWithLabeler(ctx, labeler)

	// Run stopwatch.
	startTime := time.Now()
	defer func() {
		elapsedDuration := time.Since(startTime)

		attrSet := labeler.AttributeSet()
		attrs := attrSet.ToSlice()
		code := statusWriter.status
		if code != 0 {
			codeAttr := semconv.HTTPResponseStatusCode(code)
			attrs = append(attrs, codeAttr)
			span.SetAttributes(codeAttr)
		}
		attrOpt := metric.WithAttributes(attrs...)

		// Increment request counter.
		s.requests.Add(ctx, 1, attrOpt)

		// Use floating point division here for higher precision (instead of Millisecond method).
		s.duration.Record(ctx, float64(elapsedDuration)/float64(time.Millisecond), attrOpt)
	}()

	var (
		recordError = func(stage string, err error) {
			span.RecordError(err)

			// https://opentelemetry.io/docs/specs/semconv/http/http-spans/#status
			// Span Status MUST be left unset if HTTP status code was in the 1xx, 2xx or 3xx ranges,
			// unless there was another error (e.g., network error receiving the response body; or 3xx codes with
			// max redirects exceeded), in which case status MUST be set to Error.
			code := statusWriter.status
			if code >= 100 && code < 500 {
				span.SetStatus(codes.Error, stage)
			}

			attrSet := labeler.AttributeSet()
			attrs := attrSet.ToSlice()
			if code != 0 {
				attrs = append(attrs, semconv.HTTPResponseStatusCode(code))
			}

			s.errors.Add(ctx, 1, metric.WithAttributes(attrs...))
		}
		err          error
		opErrContext = ogenerrors.OperationContext{
			Name: RenderOperation,
			ID:   "render",
		}
	)
	params, err := decodeRenderParams(args, argsEscaped, r)
	if err != nil {
		err = &ogenerrors.DecodeParamsError{
			OperationContext: opErrContext,
			Err:              err,
		}
		defer recordError("DecodeParams", err)
		s.cfg.ErrorHandler(ctx, w, r, err)
		return
	}

	var response *FlamebearerProfileV1
	if m := s.cfg.Middleware; m != nil {
		mreq := middleware.Request{
			Context:          ctx,
			OperationName:    RenderOperation,
			OperationSummary: "",
			OperationID:      "render",
			Body:             nil,
			Params: middleware.Parameters{
				{
					Name: "from",
					In:   "query",
				}: params.From,
				{
					Name: "until",
					In:   "query",
				}: params.Until,
				{
					Name: "query",
					In:   "query",
				}: params.Query,
				{
					Name: "name",
					In:   "query",
				}: params.Name,
				{
					Name: "groupBy",
					In:   "query",
				}: params.GroupBy,
				{
					Name: "maxNodes",
					In:   "query",
				}: params.MaxNodes,
				{
					Name: "format",
					In:   "query",
				}: params.Format,
			},
			Raw: r,
		}

		type (
			Request  = struct{}
			Params   = RenderParams
			Response = *FlamebearerProfileV1
		)
		response, err = middleware.HookMiddleware[
			Request,
			Params,
			Response,
		](
			m,
			mreq,
			unpackRenderParams,
			func(ctx context.Context, request Request, params Params) (response Response, err error) {
				response, err = s.h.Render(ctx, params)
				return response, err
			},
		)
	} else {
		response, err = s.h.Render(ctx, params)
	}
	if err != nil {
		if errRes, ok := errors.Into[*ErrorStatusCode](err); ok {
			if err := encodeErrorResponse(errRes, w, span); err != nil {
				defer recordError("Internal", err)
			}
			return
		}
		if errors.Is(err, ht.ErrNotImplemented) {
			s.cfg.ErrorHandler(ctx, w, r, err)
			return
		}
		if err := encodeErrorResponse(s.h.NewError(ctx, err), w, span); err != nil {
			defer recordError("Internal", err)
		}
		return
	}

	if err := encodeRenderResponse(response, w, span); err != nil {
		defer recordError("EncodeResponse", err)
		if !errors.Is(err, ht.ErrInternalServerErrorResponse) {
			s.cfg.ErrorHandler(ctx, w, r, err)
		}
		return
	}
}
