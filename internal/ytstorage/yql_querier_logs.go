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
	caps.Label.Add(logql.OpEq, logql.OpNotEq, logql.OpRe, logql.OpNotRe)
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
	// Set cluster to use.
	fmt.Fprintf(&query, "use %s;\n", q.clusterName)

	// Precompile regexps.
	for matcherIdx, m := range params.Labels {
		if m.Op.IsRegex() {
			// Note that Re2::Match is used, instead of Re2::Grep.
			//
			// Re2::Match(<pattern>) effectively the same as Re2::Grep("^"+<pattern>+"$"), i.e.
			// regexp should match whole string.
			//
			// See https://ytsaurus.tech/docs/en/yql/udf/list/pire#match
			// See https://ytsaurus.tech/docs/en/yql/udf/list/re2#match
			fmt.Fprintf(&query, "$matcher_%d = Re2::Match(%q);\n", matcherIdx, m.Re)
		}
	}

	fmt.Fprintf(&query, "SELECT * FROM %#q WHERE (`timestamp` >= %d AND `timestamp` <= %d)\n", table, start, end)
	// Preallocate path buffer.
	yp := make([]byte, 0, 32)
	for matcherIdx, m := range params.Labels {
		query.WriteString("\t")
		switch m.Op {
		case logql.OpEq, logql.OpRe:
			query.WriteString("AND (\n")
		case logql.OpNotEq, logql.OpNotRe:
			// Match record only if none of attributes columns have matching values.
			query.WriteString("AND NOT (\n")
		default:
			return nil, errors.Errorf("unexpected op %q", m.Op)
		}

		yp = append(yp[:0], '/')
		yp = append(yp, m.Label...)
		yp = append(yp, "/1"...)
		for i, column := range []string{
			"attrs",
			"scope_attrs",
			"resource_attrs",
		} {
			query.WriteString("\t\t")
			if i != 0 {
				query.WriteString("OR ")
			}

			fmt.Fprintf(&query, "( Yson::YPath(%s, %q) IS NOT NULL AND ", column, yp)
			// Note: predicate negated above.
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				fmt.Fprintf(&query, "Yson::ConvertToString(Yson::YPath(%s, %q)) = %q", column, yp, m.Value)
			case logql.OpRe, logql.OpNotRe:
				fmt.Fprintf(&query, "$matcher_%d(Yson::ConvertToString(Yson::YPath(%s, %q)))", matcherIdx, column, yp)
			default:
				return nil, errors.Errorf("unexpected op %q", m.Op)
			}
			query.WriteString(" )\n")
		}
		query.WriteString("\t)\n")
	}
	query.WriteString("ORDER BY `timestamp`")

	return yqlclient.YQLQuery[logstorage.Record](ctx, q.client, query.String())
}
