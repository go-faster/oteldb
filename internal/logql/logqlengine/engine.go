// Package logqlengine implements LogQL evaluation engine.
package logqlengine

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/logql"
)

// Engine is a LogQL evaluation engine.
type Engine struct {
	querier     Querier
	querierCaps QuerierCapabilities

	lookbackDuration time.Duration
	otelAdapter      bool
	parseOpts        logql.ParseOptions

	optimizers []Optimizer

	tracer trace.Tracer
}

// Options sets Engine options.
type Options struct {
	// LookbackDuration sets lookback duration for instant queries.
	//
	// Should be negative, otherwise default value would be used.
	LookbackDuration time.Duration

	// ParseOptions is a LogQL parser options.
	ParseOptions logql.ParseOptions

	// OTELAdapter enables 'otel adapter' whatever it is.
	OTELAdapter bool

	// Optimizers defines a list of optimiziers to use.
	Optimizers []Optimizer

	// TracerProvider provides OpenTelemetry tracer for this engine.
	TracerProvider trace.TracerProvider
}

func (o *Options) setDefaults() {
	if o.LookbackDuration >= 0 {
		o.LookbackDuration = -30 * time.Second
	}
	if o.Optimizers == nil {
		o.Optimizers = DefaultOptimizers()
	}
	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}
}

// NewEngine creates new Engine.
func NewEngine(querier Querier, opts Options) *Engine {
	opts.setDefaults()

	return &Engine{
		querier:          querier,
		querierCaps:      querier.Capabilities(),
		lookbackDuration: opts.LookbackDuration,
		otelAdapter:      opts.OTELAdapter,
		parseOpts:        opts.ParseOptions,
		optimizers:       opts.Optimizers,
		tracer:           opts.TracerProvider.Tracer("logql.Engine"),
	}
}

// ParseOptions returns [logql.ParseOptions] used by engine.
func (e *Engine) ParseOptions() logql.ParseOptions {
	return e.parseOpts
}

// NewQuery creates new [Query].
func (e *Engine) NewQuery(ctx context.Context, query string) (_ Query, rerr error) {
	ctx, span := e.tracer.Start(ctx, "logql.Engine.NewQuery", trace.WithAttributes(
		attribute.String("logql.query", query),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	expr, err := logql.Parse(query, e.parseOpts)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	q, err := e.buildQuery(ctx, expr)
	if err != nil {
		return nil, err
	}

	q, err = e.applyOptimizers(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "optimize")
	}

	return q, nil
}

func (e *Engine) buildQuery(ctx context.Context, expr logql.Expr) (_ Query, rerr error) {
	switch expr := logql.UnparenExpr(expr).(type) {
	case *logql.LogExpr:
		return e.buildLogQuery(ctx, expr)
	case *logql.LiteralExpr:
		return e.buildLiteralQuery(ctx, expr)
	case logql.MetricExpr:
		return e.buildMetricQuery(ctx, expr)
	default:
		return nil, errors.Errorf("unexpected expression %T", expr)
	}
}
