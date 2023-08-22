package ytstorage

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/yqlclient"
)

var _ logqlengine.Querier = (*YQLQuerier)(nil)

// Сapabilities defines storage capabilities.
func (q *YQLQuerier) Сapabilities() (caps logqlengine.QuerierСapabilities) {
	caps.Label.Add(logql.OpEq, logql.OpNotEq)
	return caps
}

// SelectLogs makes a query for LogQL engine.
func (q *YQLQuerier) SelectLogs(ctx context.Context, start, end otelstorage.Timestamp, params logqlengine.SelectLogsParams) (_ iterators.Iterator[logstorage.Record], rerr error) {
	table := q.tables.logs

	ctx, span := q.tracer.Start(ctx, "SelectLogs",
		trace.WithAttributes(
			attribute.Int("ytstorage.labels_count", len(params.Labels)),
			attribute.Int64("ytstorage.start_range", int64(start)),
			attribute.Int64("ytstorage.end_range", int64(end)),
			attribute.Stringer("ytstorage.table", table),
			attribute.String("ytstorage.yql.cluster_name", q.clusterName),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var query strings.Builder
	fmt.Fprintf(&query, "use %s;\nSELECT * FROM %#q WHERE (`timestamp` >= %d AND `timestamp` <= %d)", q.clusterName, table, start, end)
	// Preallocate path buffer.
	yp := make([]byte, 0, 32)
	for _, m := range params.Labels {
		query.WriteString(" AND (")

		yp = append(yp[:0], '/')
		yp = append(yp, m.Label...)
		yp = append(yp, "/1"...)
		for i, column := range []string{
			"attrs",
			"scope_attrs",
			"resource_attrs",
		} {
			if i != 0 {
				query.WriteString(" OR ")
			}

			switch m.Op {
			case logql.OpEq:
				fmt.Fprintf(&query, "Yson::ConvertToString(Yson::YPath(%s, %q)) = %q", column, yp, m.Value)
			case logql.OpNotEq:
				fmt.Fprintf(&query, "Yson::ConvertToString(Yson::YPath(%s, %q)) != %q", column, yp, m.Value)
			default:
				return nil, errors.Errorf("unexpected op %q", m.Op)
			}
		}
		query.WriteByte(')')
	}
	query.WriteString("ORDER BY `timestamp`")

	return yqlclient.YQLQuery[logstorage.Record](ctx, q.client, query.String())
}
