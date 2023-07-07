package ytstorage

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/exp/maps"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

var _ logstorage.Querier = (*YTQLQuerier)(nil)

// LabelNames returns all available label names.
func (q *YTQLQuerier) LabelNames(ctx context.Context, _ logstorage.LabelsOptions) ([]string, error) {
	// FIXME(tdakkota): use time range from opts
	query := fmt.Sprintf("name FROM [%s]", q.tables.logLabels)
	names := map[string]struct{}{}
	err := queryRows(ctx, q.yc, query, func(label logstorage.Label) {
		names[label.Name] = struct{}{}
	})
	return maps.Keys(names), err
}

// LabelValues returns all available label values for given label.
func (q *YTQLQuerier) LabelValues(ctx context.Context, labelName string, _ logstorage.LabelsOptions) (iterators.Iterator[logstorage.Label], error) {
	// FIXME(tdakkota): use time range from opts
	query := fmt.Sprintf("* FROM [%s] WHERE name = %q", q.tables.logLabels, labelName)
	r, err := q.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[logstorage.Label]{reader: r}, nil
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
func (q *YTQLQuerier) SelectLogs(ctx context.Context, start, end otelstorage.Timestamp, params logqlengine.SelectLogsParams) (iterators.Iterator[logstorage.Record], error) {
	var query strings.Builder
	fmt.Fprintf(&query, "* FROM [%s] WHERE (timestamp >= %d AND timestamp <= %d)", q.tables.logs, start, end)
	for _, m := range params.Labels {
		query.WriteString(" AND (")
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

			switch m.Op {
			case logql.OpEq:
				fmt.Fprintf(&query, "try_get_string(%s, %q) = %q", column, yp, m.Value)
			case logql.OpNotEq:
				fmt.Fprintf(&query, "try_get_string(%s, %q) != %q", column, yp, m.Value)
			default:
				return nil, errors.Errorf("unexpected op %q", m.Op)
			}
		}
		query.WriteByte(')')
	}

	r, err := q.yc.SelectRows(ctx, query.String(), nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[logstorage.Record]{reader: r}, nil
}
