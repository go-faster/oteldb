package logqlengine

import (
	"context"
	"fmt"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlerrors"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

// MetricQuery represents a metric query.
type MetricQuery struct {
	Root MetricNode

	tracer trace.Tracer
}

func (e *Engine) buildMetricQuery(ctx context.Context, expr logql.MetricExpr) (Query, error) {
	root, err := e.buildMetricNode(ctx, expr)
	if err != nil {
		return nil, err
	}

	return &MetricQuery{
		Root:   root,
		tracer: e.tracer,
	}, nil
}

var _ Query = (*MetricQuery)(nil)

// Eval implements [Query].
func (q *MetricQuery) Eval(ctx context.Context, params EvalParams) (lokiapi.QueryResponseData, error) {
	data, err := q.eval(ctx, params)
	if err != nil {
		return data, errors.Wrap(err, "evaluate metric query")
	}
	return data, nil
}

func (q *MetricQuery) eval(ctx context.Context, params EvalParams) (data lokiapi.QueryResponseData, rerr error) {
	ctx, span := q.tracer.Start(ctx, "logql.MetricQuery", trace.WithAttributes(
		attribute.Int64("logql.params.start", params.Start.UnixNano()),
		attribute.Int64("logql.params.end", params.End.UnixNano()),
		attribute.Int64("logql.params.step", int64(params.Step)),
		attribute.Stringer("logql.params.direction", params.Direction),
		attribute.Int("logql.params.limit", params.Limit),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	iter, err := q.Root.EvalMetric(ctx, MetricParams{
		// NOTE(tdakkota): for some reason, timestamps in Loki appear truncated by step.
		// 	Do the same thing.
		Start: params.Start.Truncate(params.Step),
		End:   params.End.Truncate(params.Step).Add(params.Step),
		Step:  params.Step,
	})
	if err != nil {
		return data, err
	}
	defer func() {
		_ = iter.Close()
	}()

	span.AddEvent("read_result")
	data, err = logqlmetric.ReadStepResponse(iter, params.IsInstant())
	if err != nil {
		return data, err
	}
	span.AddEvent("return_result", trace.WithAttributes(
		attribute.String("logql.data.type", string(data.Type)),
	))

	return data, nil
}

func (e *Engine) buildMetricNode(ctx context.Context, expr logql.MetricExpr) (MetricNode, error) {
	switch expr := logql.UnparenExpr(expr).(type) {
	case *logql.RangeAggregationExpr:
		node, err := e.buildSampleNode(ctx, expr)
		if err != nil {
			return nil, err
		}
		return &RangeAggregation{
			Input: node,
			Expr:  expr,
		}, nil
	case *logql.VectorAggregationExpr:
		node, err := e.buildMetricNode(ctx, expr.Expr)
		if err != nil {
			return nil, err
		}
		return &VectorAggregation{
			Input: node,
			Expr:  expr,
		}, nil
	case *logql.LiteralExpr:
	case *logql.LabelReplaceExpr:
		node, err := e.buildMetricNode(ctx, expr.Expr)
		if err != nil {
			return nil, err
		}
		return &LabelReplace{
			Input: node,
			Expr:  expr,
		}, nil
	case *logql.VectorExpr:
		return &Vector{
			Expr: expr,
		}, nil
	case *logql.BinOpExpr:
		if lit, ok := expr.Left.(*logql.LiteralExpr); ok {
			right, err := e.buildMetricNode(ctx, expr.Right)
			if err != nil {
				return nil, err
			}
			return &LiteralBinOp{
				Input:           right,
				Literal:         lit.Value,
				IsLiteralOnLeft: true,
				Expr:            expr,
			}, nil
		}
		if lit, ok := expr.Right.(*logql.LiteralExpr); ok {
			left, err := e.buildMetricNode(ctx, expr.Left)
			if err != nil {
				return nil, err
			}
			return &LiteralBinOp{
				Input:           left,
				Literal:         lit.Value,
				IsLiteralOnLeft: false,
				Expr:            expr,
			}, nil
		}

		left, err := e.buildMetricNode(ctx, expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.buildMetricNode(ctx, expr.Right)
		if err != nil {
			return nil, err
		}
		return &BinOp{
			Left:  left,
			Right: right,
			Expr:  expr,
		}, nil
	default:
		return nil, errors.Errorf("unexpected expression %T", expr)
	}
	return nil, &logqlerrors.UnsupportedError{Msg: fmt.Sprintf("expression %T is not supported yet", expr)}
}
