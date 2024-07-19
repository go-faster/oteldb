package tracestorage

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Consumer consumes given traces and inserts them using given Inserter.
type Consumer struct {
	inserter Inserter
}

// NewConsumer creates new Consumer.
func NewConsumer(i Inserter) *Consumer {
	return &Consumer{
		inserter: i,
	}
}

// ConsumeTraces implements otelreceiver.Consumer.
func (c *Consumer) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	w, err := c.inserter.SpanWriter(ctx)
	if err != nil {
		return errors.Wrap(err, "create span writer")
	}
	defer func() {
		_ = w.Close()
	}()

	resSpans := traces.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		batchID := uuid.New()
		resSpan := resSpans.At(i)
		res := resSpan.Resource()

		scopeSpans := resSpan.ScopeSpans()
		for i := 0; i < scopeSpans.Len(); i++ {
			scopeSpan := scopeSpans.At(i)
			scope := scopeSpan.Scope()

			spans := scopeSpan.Spans()
			for i := 0; i < spans.Len(); i++ {
				span := spans.At(i)
				if err := w.Add(NewSpanFromOTEL(batchID, res, scope, span)); err != nil {
					return errors.Wrap(err, "write span")
				}
			}
		}
	}

	if err := w.Submit(ctx); err != nil {
		return errors.Wrap(err, "submit spans")
	}
	return nil
}
