package chstorage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

type SearchAttributesOperation byte

const (
	SearchAttributesOperationEq SearchAttributesOperation = iota
	SearchAttributesOperationNotEq
	SearchAttributesOperationRe
	SearchAttributesOperationNotRe
)

type SearchAttributesMatcher struct {
	Operation SearchAttributesOperation
	Key       string
	Value     string
}

type SearchAttributes struct {
	Start    time.Time
	End      time.Time
	Matchers []SearchAttributesMatcher
}

func Hashes(attrs []AttributesRow) []otelstorage.Hash {
	out := make([]otelstorage.Hash, 0, len(attrs))
	for _, attr := range attrs {
		out = append(out, attr.Hash)
	}
	return out
}

func (q *Querier) getAttributes(ctx context.Context, attrs SearchAttributes) ([]AttributesRow, error) {
	ctx, span := q.tracer.Start(ctx, "getAttributes")
	defer span.End()

	table := q.tables.Resources
	const name = "attributes"
	c := NewAttributes(name)

	var query strings.Builder
	fmt.Fprintf(&query, "SELECT %[1]s FROM %#[2]q", c.Columns().All(), table)
	if len(attrs.Matchers) > 0 {
		query.WriteString(" WHERE true")
	}
	for _, m := range attrs.Matchers {
		switch m.Operation {
		case SearchAttributesOperationEq, SearchAttributesOperationRe:
			query.WriteString(" AND ")
		case SearchAttributesOperationNotEq, SearchAttributesOperationNotRe:
			query.WriteString(" AND NOT ")
		default:
			return nil, errors.Errorf("unexpected type %q", m.Operation)
		}
		// Note: predicate negated above.
		key, value := singleQuoted(m.Key), singleQuoted(m.Value)
		switch m.Operation {
		case SearchAttributesOperationEq, SearchAttributesOperationNotEq:
			fmt.Fprintf(&query, "%s[%s] = %s", name, key, value)
		case SearchAttributesOperationRe, SearchAttributesOperationNotRe:
			fmt.Fprintf(&query, "match(%s[%s], %s)", name, key, value)
		default:
			return nil, errors.Errorf("unexpected type %q", m.Operation)
		}
	}

	fmt.Println(query.String())

	var out []AttributesRow
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query.String(),
		Result: c.Columns().Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := range c.Hash {
				row, err := c.Row(i)
				if err != nil {
					return errors.Wrap(err, "row")
				}
				out = append(out, AttributesRow{
					Hash:  row.Hash(),
					Value: row,
				})
			}
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "select")
	}

	span.AddEvent("gotAttributes",
		trace.WithAttributes(
			attribute.Int("count", len(out)),
		),
	)

	return out, nil
}
