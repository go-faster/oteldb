package ytstorage

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

var _ logstorage.Querier = (*YTQLQuerier)(nil)

// LabelNames returns all available label names.
func (q *YTQLQuerier) LabelNames(ctx context.Context, opts logstorage.LabelsOptions) (_ []string, rerr error) {
	table := q.tables.logLabels

	ctx, span := q.tracer.Start(ctx, "LabelNames",
		trace.WithAttributes(
			attribute.Int64("ytstorage.start_range", int64(opts.Start)),
			attribute.Int64("ytstorage.end_range", int64(opts.End)),
			attribute.Stringer("ytstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	// FIXME(tdakkota): use time range from opts
	query := fmt.Sprintf("name FROM [%s]", table)
	names := map[string]struct{}{}
	err := queryRows(ctx, q.yc, q.tracer, query, func(label logstorage.Label) {
		names[label.Name] = struct{}{}
	})
	return maps.Keys(names), err
}

// LabelValues returns all available label values for given label.
func (q *YTQLQuerier) LabelValues(ctx context.Context, labelName string, opts logstorage.LabelsOptions) (_ iterators.Iterator[logstorage.Label], rerr error) {
	table := q.tables.logLabels

	ctx, span := q.tracer.Start(ctx, "LabelValues",
		trace.WithAttributes(
			attribute.String("ytstorage.label_to_query", labelName),
			attribute.Int64("ytstorage.start_range", int64(opts.Start)),
			attribute.Int64("ytstorage.end_range", int64(opts.End)),
			attribute.Stringer("ytstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	// FIXME(tdakkota): use time range from opts
	query := fmt.Sprintf("* FROM [%s] WHERE name = %q", table, labelName)
	r, err := q.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return nil, err
	}

	return newYTIterator[logstorage.Label](ctx, r, q.tracer), nil
}

var _ logqlengine.Querier = (*YTQLQuerier)(nil)

// Сapabilities defines storage capabilities.
func (q *YTQLQuerier) Сapabilities() (caps logqlengine.QuerierСapabilities) {
	// FIXME(tdakkota): we don't add OpRe and OpNotRe because YT QL query executer throws an exception
	//	when regexp function are used.
	caps.Label.Add(logql.OpEq, logql.OpNotEq)
	return caps
}

// SelectLogs makes a query for LogQL engine.
func (q *YTQLQuerier) SelectLogs(ctx context.Context, start, end otelstorage.Timestamp, params logqlengine.SelectLogsParams) (_ iterators.Iterator[logstorage.Record], rerr error) {
	table := q.tables.logs

	ctx, span := q.tracer.Start(ctx, "SelectLogs",
		trace.WithAttributes(
			attribute.Int("ytstorage.labels_count", len(params.Labels)),
			attribute.Int64("ytstorage.start_range", int64(start)),
			attribute.Int64("ytstorage.end_range", int64(end)),
			attribute.Stringer("ytstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var query strings.Builder
	fmt.Fprintf(&query, "* FROM [%s] WHERE (timestamp >= %d AND timestamp <= %d)", table, start, end)
	for _, m := range params.Labels {
		switch m.Op {
		case logql.OpEq:
			query.WriteString(" AND (")
		case logql.OpNotEq:
			query.WriteString(" AND NOT (")
		default:
			return nil, errors.Errorf("unexpected op %q", m.Op)
		}

		for i, column := range []string{
			"attrs",
			"scope_attrs",
			"resource_attrs",
		} {
			if i != 0 {
				query.WriteString(" OR ")
			}
			yp := append([]byte{'/'}, m.Label...)
			yp = append(yp, "/1"...)

			fmt.Fprintf(&query, "try_get_string(%s, %q) = %q", column, yp, m.Value)
		}
		query.WriteByte(')')
	}

	r, err := q.yc.SelectRows(ctx, query.String(), nil)
	if err != nil {
		return nil, err
	}
	return newYTIterator[logstorage.Record](ctx, r, q.tracer), nil
}
