package chstorage

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

var _ storage.ExemplarQueryable = (*Querier)(nil)

// Querier returns a new Querier on the storage.
func (q *Querier) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return &exemplarQuerier{
		ctx: ctx,

		ch:              q.ch,
		tables:          q.tables,
		getLabelMapping: q.getMetricsLabelMapping,

		tracer: q.tracer,
	}, nil
}

type exemplarQuerier struct {
	ctx context.Context

	ch              ClickhouseClient
	tables          Tables
	getLabelMapping func(context.Context, []string) (map[string]string, error)

	tracer trace.Tracer
}

var _ storage.ExemplarQuerier = (*exemplarQuerier)(nil)

func (q *exemplarQuerier) Select(startMs, endMs int64, matcherSets ...[]*labels.Matcher) (_ []exemplar.QueryResult, rerr error) {
	table := q.tables.Exemplars

	var start, end time.Time
	if startMs >= 0 {
		start = time.UnixMilli(startMs)
	}
	if endMs >= 0 {
		end = time.UnixMilli(endMs)
	}

	ctx, span := q.tracer.Start(q.ctx, "Select",
		trace.WithAttributes(
			attribute.Int64("chstorage.range.start", start.UnixNano()),
			attribute.Int64("chstorage.range.end", end.UnixNano()),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var queryLabels []string
	for _, set := range matcherSets {
		for _, m := range set {
			queryLabels = append(queryLabels, m.Name)
		}
	}
	mapping, err := q.getLabelMapping(ctx, queryLabels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	buildQuery := func(table string) (string, error) {
		var query strings.Builder
		fmt.Fprintf(&query, `SELECT %[1]s FROM %#[2]q WHERE true
		`, newExemplarColumns().Columns().All(), table)
		if !start.IsZero() {
			fmt.Fprintf(&query, "\tAND toUnixTimestamp64Nano(timestamp) >= %d\n", start.UnixNano())
		}
		if !end.IsZero() {
			fmt.Fprintf(&query, "\tAND toUnixTimestamp64Nano(timestamp) <= %d\n", end.UnixNano())
		}
		query.WriteString("AND ( false\n")
		for i, set := range matcherSets {
			fmt.Fprintf(&query, "OR ( true -- matcher set %d\n", i)
			for _, m := range set {
				switch m.Type {
				case labels.MatchEqual, labels.MatchRegexp:
					query.WriteString(" AND ")
				case labels.MatchNotEqual, labels.MatchNotRegexp:
					query.WriteString(" AND NOT ")
				default:
					return "", errors.Errorf("unexpected type %q", m.Type)
				}

				{
					selectors := []string{
						"name",
					}
					if name := m.Name; name != labels.MetricName {
						if mapped, ok := mapping[name]; ok {
							name = mapped
						}
						selectors = []string{
							fmt.Sprintf("JSONExtractString(attributes, %s)", singleQuoted(name)),
							fmt.Sprintf("JSONExtractString(resource, %s)", singleQuoted(name)),
						}
					}
					query.WriteString("(\n")
					for i, sel := range selectors {
						if i != 0 {
							query.WriteString(" OR ")
						}
						// Note: predicate negated above.
						switch m.Type {
						case labels.MatchEqual, labels.MatchNotEqual:
							fmt.Fprintf(&query, "%s = %s\n", sel, singleQuoted(m.Value))
						case labels.MatchRegexp, labels.MatchNotRegexp:
							fmt.Fprintf(&query, "%s REGEXP %s\n", sel, singleQuoted(m.Value))
						default:
							return "", errors.Errorf("unexpected type %q", m.Type)
						}
					}
					query.WriteString(")\n")
				}
			}
			query.WriteString(")\n")
		}
		query.WriteString(") ORDER BY timestamp")
		return query.String(), nil
	}

	query, err := buildQuery(table)
	if err != nil {
		return nil, err
	}
	type groupedExemplars struct {
		labels    map[string]string
		exemplars []exemplar.Exemplar
	}
	var (
		c   = newExemplarColumns()
		set = map[seriesKey]*groupedExemplars{}
		lb  labels.ScratchBuilder
	)
	if err := q.ch.Do(ctx, ch.Query{
		Body:   query,
		Result: c.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				var (
					name               = c.name.Row(i)
					filteredAttributes = c.filteredAttributes.Row(i)
					exemplarTimestamp  = c.exemplarTimestamp.Row(i)
					value              = c.value.Row(i)
					spanID             = c.spanID.Row(i)
					traceID            = c.traceID.Row(i)
					attributes         = c.attributes.Row(i)
					resource           = c.resource.Row(i)
				)
				if err != nil {
					return errors.Wrap(err, "decode attributes")
				}
				if err != nil {
					return errors.Wrap(err, "decode resource")
				}
				key := seriesKey{
					name:       name,
					attributes: attributes.Hash(),
					resource:   resource.Hash(),
				}
				s, ok := set[key]
				if !ok {
					s = &groupedExemplars{
						labels: map[string]string{},
					}
					set[key] = s
				}

				exemplarLabels := map[string]string{
					"span_id":  hex.EncodeToString(spanID[:]),
					"trace_id": hex.EncodeToString(traceID[:]),
				}
				if err := parseLabels(filteredAttributes, exemplarLabels); err != nil {
					return errors.Wrap(err, "parse filtered attributes")
				}
				s.exemplars = append(s.exemplars, exemplar.Exemplar{
					Labels: buildPromLabels(&lb, exemplarLabels),
					Value:  value,
					Ts:     exemplarTimestamp.UnixMilli(),
					HasTs:  true,
				})

				s.labels[labels.MetricName] = otelstorage.KeyToLabel(name)
				attrsToLabels(attributes, s.labels)
				attrsToLabels(resource, s.labels)
			}
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "do query")
	}

	result := make([]exemplar.QueryResult, 0, len(set))
	for _, group := range set {
		result = append(result, exemplar.QueryResult{
			SeriesLabels: buildPromLabels(&lb, group.labels),
			Exemplars:    group.exemplars,
		})
	}
	return result, nil
}
