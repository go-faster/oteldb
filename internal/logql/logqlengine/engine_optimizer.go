package logqlengine

import (
	"context"

	"github.com/go-faster/errors"
)

// Optimizer defines an interface for optimizer.
type Optimizer interface {
	// Name returns optimizer name.
	Name() string
	Optimize(ctx context.Context, q Query) (Query, error)
}

// DefaultOptimizers returns slice of default [Optimizer]s.
func DefaultOptimizers() []Optimizer {
	return []Optimizer{}
}

func (e *Engine) applyOptimizers(ctx context.Context, q Query) (Query, error) {
	var err error
	for _, o := range e.optimizers {
		q, err = o.Optimize(ctx, q)
		if err != nil {
			return nil, errors.Wrapf(err, "optimizer %q failed", o.Name())
		}
	}
	return q, nil
}
