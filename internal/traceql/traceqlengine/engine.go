// Package traceqlengine implements TraceQL evaluation engine.
package traceqlengine

import (
	"context"
	"fmt"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

// Engine is a TraceQL evaluation engine.
type Engine struct {
	querier Querier

	tracer trace.Tracer
}

// Options sets Engine options.
type Options struct {
	// TracerProvider provides OpenTelemetry tracer for this engine.
	TracerProvider trace.TracerProvider
}

func (o *Options) setDefaults() {
	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}
}

// NewEngine creates new Engine.
func NewEngine(querier Querier, opts Options) *Engine {
	opts.setDefaults()

	return &Engine{
		querier: querier,
		tracer:  opts.TracerProvider.Tracer("traceql.Engine"),
	}
}

// EvalParams sets evaluation parameters.
type EvalParams struct {
	// Trace duration to search, optional.
	MinDuration time.Duration
	MaxDuration time.Duration
	// Time range to search, optional.
	Start otelstorage.Timestamp
	End   otelstorage.Timestamp
	Limit int
}

// Eval parses and evaluates query.
func (e *Engine) Eval(ctx context.Context, query string, params EvalParams) (_ *tempoapi.Traces, rerr error) {
	ctx, span := e.tracer.Start(ctx, "Eval",
		trace.WithAttributes(
			attribute.String("traceql.query", query),
			attribute.Int64("traceql.min_duration", int64(params.MinDuration)),
			attribute.Int64("traceql.max_duration", int64(params.MaxDuration)),
			attribute.Int64("traceql.start", int64(params.Start)),
			attribute.Int64("traceql.end", int64(params.End)),
			attribute.Int("traceql.limit", params.Limit),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	expr, err := traceql.Parse(query)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	return e.evalExpr(ctx, expr, params)
}

func (e *Engine) evalExpr(ctx context.Context, expr traceql.Expr, params EvalParams) (*tempoapi.Traces, error) {
	stages, ok := expr.(*traceql.SpansetPipeline)
	if !ok {
		return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported expression %T", expr)}
	}

	processor, err := BuildPipeline(stages.Pipeline...)
	if err != nil {
		return nil, errors.Wrap(err, "build pipeline")
	}

	iter, err := e.querier.SelectSpansets(
		ctx,
		extractPredicates(expr, params),
	)
	if err != nil {
		return nil, errors.Wrap(err, "select spansets")
	}
	defer func() {
		_ = iter.Close()
	}()

	var (
		limit  = params.Limit
		elem   Trace
		result []tempoapi.TraceSearchMetadata
	)
	if limit < 0 {
		limit = 20
	}
	for iter.Next(&elem) {
		if len(result) >= limit {
			break
		}
		if len(elem.Spans) < 1 {
			continue
		}

		var (
			root  = elem.Spans[0]
			start = root.Start.AsTime()
			end   = root.End.AsTime()
		)
		for _, span := range elem.Spans[1:] {
			if st := span.Start.AsTime(); st.Before(start) {
				start = st
			}
			if et := span.End.AsTime(); et.After(end) {
				end = et
			}
			if !root.ParentSpanID.IsEmpty() && span.ParentSpanID.IsEmpty() {
				root = span
			}
		}

		var rootServiceName string
		if attrs := root.ResourceAttrs; !attrs.IsZero() {
			if v, ok := attrs.AsMap().Get("service.name"); ok && v.Type() == pcommon.ValueTypeStr {
				rootServiceName = v.Str()
			}
		}

		ss := []Spanset{
			{
				TraceID:         elem.TraceID,
				Spans:           elem.Spans,
				RootSpanName:    root.Name,
				RootServiceName: rootServiceName,
				Start:           start,
				TraceDuration:   end.Sub(start),
			},
		}

		var err error
		ss, err = processor.Process(ss)
		if err != nil {
			return nil, errors.Wrapf(err, "process trace %s", elem.TraceID.Hex())
		}

		for _, s := range ss {
			if len(result) >= limit {
				break
			}

			var spans tempoapi.TempoSpanSet
			for _, span := range s.Spans {
				spans.Spans = append(spans.Spans, span.AsTempoSpan())
			}

			// Add attributes from root.
			tracestorage.ConvertToTempoAttrs(&spans.Attributes, root.ScopeAttrs)
			tracestorage.ConvertToTempoAttrs(&spans.Attributes, root.ResourceAttrs)
			result = append(result, tempoapi.TraceSearchMetadata{
				TraceID:           s.TraceID.Hex(),
				RootServiceName:   tempoapi.NewOptString(s.RootServiceName),
				RootTraceName:     tempoapi.NewOptString(s.RootSpanName),
				StartTimeUnixNano: s.Start,
				DurationMs:        tempoapi.NewOptInt(int(s.TraceDuration.Milliseconds())),
				SpanSet:           tempoapi.NewOptTempoSpanSet(spans),
			})
		}
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}
	return &tempoapi.Traces{Traces: result}, nil
}
