package tracestorage

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/internal/traceql"
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
	tags := map[Tag]struct{}{}
	addTags := func(attrs pcommon.Map, scope traceql.AttributeScope) {
		attrs.Range(func(k string, v pcommon.Value) bool {
			switch t := v.Type(); t {
			case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
			default:
				tags[Tag{
					k,
					v.AsString(),
					traceql.StaticTypeFromValueType(t),
					scope,
				}] = struct{}{}
			}
			return true
		})
	}

	var (
		insertBatch []Span
		resSpans    = traces.ResourceSpans()
	)
	for i := 0; i < resSpans.Len(); i++ {
		batchID := uuid.New()
		resSpan := resSpans.At(i)
		res := resSpan.Resource()
		addTags(res.Attributes(), traceql.ScopeResource)

		scopeSpans := resSpan.ScopeSpans()
		for i := 0; i < scopeSpans.Len(); i++ {
			scopeSpan := scopeSpans.At(i)
			scope := scopeSpan.Scope()
			addTags(scope.Attributes(), traceql.ScopeInstrumentation)

			spans := scopeSpan.Spans()
			for i := 0; i < spans.Len(); i++ {
				span := spans.At(i)
				insertBatch = append(insertBatch, NewSpanFromOTEL(batchID, res, scope, span))
				addTags(span.Attributes(), traceql.ScopeSpan)
			}
		}
	}

	if err := c.inserter.InsertSpans(ctx, insertBatch); err != nil {
		return errors.Wrap(err, "insert spans")
	}
	if err := c.inserter.InsertTags(ctx, tags); err != nil {
		return errors.Wrap(err, "insert tags")
	}
	return nil
}
