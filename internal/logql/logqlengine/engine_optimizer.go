package logqlengine

import (
	"context"

	"github.com/go-faster/errors"
)

// Optimizer defines an interface for optimizer.
type Optimizer interface {
	// Name returns optimizer name.
	Name() string
	Optimize(ctx context.Context, q Query, opts OptimizeOptions) (Query, error)
}

// OptimizeOptions defines options for [Optimizer.Optimize].
type OptimizeOptions struct {
	Explain bool
}

// DefaultOptimizers returns slice of default [Optimizer]s.
func DefaultOptimizers() []Optimizer {
	return []Optimizer{}
}

func (e *Engine) applyOptimizers(ctx context.Context, q Query, opts OptimizeOptions) (_ Query, rerr error) {
	ctx, span := e.tracer.Start(ctx, "logql.Engine.applyOptimizers")
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var err error
	for _, o := range e.optimizers {
		q, err = o.Optimize(ctx, q, opts)
		if err != nil {
			return nil, errors.Wrapf(err, "optimizer %q failed", o.Name())
		}
	}
	return q, nil
}
