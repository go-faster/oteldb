// Package logqlengine implements LogQL evaluation engine.
package logqlengine

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Engine is a LogQL evaluation engine.
type Engine struct {
	querier Querier

	tracer    trace.Tracer
	parseOpts logql.ParseOptions
}

// Options sets Engine options.
type Options struct {
	TracerProvider trace.TracerProvider

	ParseOptions logql.ParseOptions
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
		querier:   querier,
		tracer:    opts.TracerProvider.Tracer("logql.Engine"),
		parseOpts: opts.ParseOptions,
	}
}

// EvalParams sets evaluation parameters.
type EvalParams struct {
	Start     otelstorage.Timestamp
	End       otelstorage.Timestamp
	Direction string // forward, backward
}

// Eval parses and evaluates query.
func (e *Engine) Eval(ctx context.Context, query string, params EvalParams) (s lokiapi.Streams, rerr error) {
	ctx, span := e.tracer.Start(ctx, "Eval",
		trace.WithAttributes(
			attribute.String("logql.query", query),
			attribute.Int64("logql.start", int64(params.Start)),
			attribute.Int64("logql.end", int64(params.End)),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	expr, err := logql.Parse(query, e.parseOpts)
	if err != nil {
		return s, errors.Wrap(err, "parse")
	}

	logExpr, ok := logql.UnparenExpr(expr).(*logql.LogExpr)
	if !ok {
		return s, errors.Errorf("expression %T is not supported yet", expr)
	}

	pipeline, err := BuildPipeline(logExpr.Pipeline...)
	if err != nil {
		return s, errors.Wrap(err, "build pipeline")
	}

	iter, err := e.querier.SelectLogs(ctx,
		params.Start,
		params.End,
		SelectLogsParams{
			Labels: logExpr.Sel.Matchers,
		},
	)
	if err != nil {
		return s, errors.Wrap(err, "query")
	}
	defer func() {
		_ = iter.Close()
	}()

	var (
		lg = zctx.From(ctx)

		record logstorage.Record
		set    LabelSet

		streams = map[string]lokiapi.Stream{}
	)
	for iter.Next(&record) {
		if err := set.SetAttrs(
			record.Attrs,
			record.ScopeAttrs,
			record.ResourceAttrs,
		); err != nil {
			lg.Warn("Invalid label name", zap.Uint64("ts", uint64(record.Timestamp)), zap.Error(err))
			// Just skip the line.
			continue
		}

		newLine, keep := pipeline.Process(record.Timestamp, record.Body, set)
		if !keep {
			continue
		}

		// FIXME(tdakkota): allocates a string for every record.
		key := set.String()
		stream, ok := streams[key]
		if !ok {
			stream = lokiapi.Stream{
				Stream: lokiapi.NewOptLabelSet(set.AsLokiAPI()),
			}
		}
		stream.Values = append(stream.Values, lokiapi.Entry{
			{T: uint64(record.Timestamp), V: newLine},
		})
		streams[key] = stream
	}

	return maps.Values(streams), nil
}
