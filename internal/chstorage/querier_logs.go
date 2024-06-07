package chstorage

import (
	"context"
	"slices"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/xattribute"
)

var (
	_ logstorage.Querier  = (*Querier)(nil)
	_ logqlengine.Querier = (*Querier)(nil)
)

// LabelNames implements logstorage.Querier.
func (q *Querier) LabelNames(ctx context.Context, opts logstorage.LabelsOptions) (_ []string, rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.LabelNames",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		names proto.ColStr
		dedup = map[string]struct{}{
			// Add materialized labels.
			logstorage.LabelTraceID:           {},
			logstorage.LabelSpanID:            {},
			logstorage.LabelSeverity:          {},
			logstorage.LabelBody:              {},
			logstorage.LabelServiceName:       {},
			logstorage.LabelServiceInstanceID: {},
			logstorage.LabelServiceNamespace:  {},
		}
	)
	if err := q.do(ctx, selectQuery{
		Query: chsql.Select(table,
			chsql.ResultColumn{
				Name: "names",
				Expr: chsql.ArrayJoin(
					chsql.ArrayConcat(
						attrKeys(colAttrs),
						attrKeys(colResource),
						attrKeys(colScope),
					),
				),
				Data: &names,
			},
		).
			Distinct(true).
			Where(
				chsql.InTimeRange("timestamp", opts.Start, opts.End),
			).
			Limit(1000),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < names.Rows(); i++ {
				// TODO: add configuration option
				name := otelstorage.KeyToLabel(names.Row(i))
				dedup[name] = struct{}{}
			}
			return nil
		},

		Type:   "LabelNames",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	// Deduplicate.
	out := maps.Keys(dedup)
	slices.Sort(out)

	return out, nil
}

type labelStaticIterator struct {
	name   string
	values []string
}

func (l *labelStaticIterator) Next(t *logstorage.Label) bool {
	if len(l.values) == 0 {
		return false
	}
	t.Name = l.name
	t.Value = l.values[0]
	l.values = l.values[1:]
	return true
}

func (l *labelStaticIterator) Err() error   { return nil }
func (l *labelStaticIterator) Close() error { return nil }

// LabelValues implements logstorage.Querier.
func (q *Querier) LabelValues(ctx context.Context, labelName string, opts logstorage.LabelsOptions) (_ iterators.Iterator[logstorage.Label], rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.LabelValues",
		trace.WithAttributes(
			attribute.String("chstorage.label", labelName),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	switch labelName {
	case logstorage.LabelBody, logstorage.LabelSpanID, logstorage.LabelTraceID:
		return &labelStaticIterator{
			name:   labelName,
			values: nil,
		}, nil
	case logstorage.LabelSeverity:
		return &labelStaticIterator{
			name: labelName,
			values: []string{
				plog.SeverityNumberUnspecified.String(),
				plog.SeverityNumberTrace.String(),
				plog.SeverityNumberDebug.String(),
				plog.SeverityNumberInfo.String(),
				plog.SeverityNumberWarn.String(),
				plog.SeverityNumberError.String(),
				plog.SeverityNumberFatal.String(),
			},
		}, nil
	}
	{
		mapping, err := q.getLabelMapping(ctx, []string{labelName})
		if err != nil {
			return nil, errors.Wrap(err, "get label mapping")
		}
		if key, ok := mapping[labelName]; ok {
			labelName = key
		}
	}

	var (
		out    []string
		values = new(proto.ColStr).Array()
	)
	if err := q.do(ctx, selectQuery{
		Query: chsql.Select(table, chsql.ResultColumn{
			Name: "values",
			Expr: chsql.Array(
				attrSelector(colAttrs, labelName),
				attrSelector(colResource, labelName),
				attrSelector(colScope, labelName),
			),
			Data: values,
		}).
			Distinct(true).
			Where(chsql.InTimeRange("timestamp", opts.Start, opts.End)).
			Limit(1000),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < values.Rows(); i++ {
				for _, v := range values.Row(i) {
					if v == "" {
						continue
					}
					out = append(out, v)
				}
			}
			return nil
		},

		Type:   "LabelValues",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return &labelStaticIterator{
		name:   labelName,
		values: out,
	}, nil
}

func (q *Querier) getLabelMapping(ctx context.Context, labels []string) (_ map[string]string, rerr error) {
	table := q.tables.LogAttrs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.getLabelMapping",
		trace.WithAttributes(
			attribute.StringSlice("chstorage.labels", labels),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		out   = make(map[string]string, len(labels))
		attrs = newLogAttrMapColumns()

		inputData proto.ColStr
	)
	for _, label := range labels {
		inputData.Append(label)
	}
	if err := q.do(ctx, selectQuery{
		Query: chsql.Select(q.tables.LogAttrs, attrs.ChsqlResult()...).
			Where(chsql.In(
				chsql.Ident("name"),
				chsql.Ident("labels"),
			)),
		OnResult: func(ctx context.Context, block proto.Block) error {
			attrs.ForEach(func(name, key string) {
				out[name] = key
			})
			return nil
		},
		ExternalTable: "labels",
		ExternalData: []proto.InputColumn{
			{Name: "name", Data: &inputData},
		},

		Type:   "LabelMapping",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return out, nil
}

func (q *Querier) getMaterializedLabelColumn(labelName string) (column chsql.Expr, isColumn bool) {
	switch labelName {
	case logstorage.LabelTraceID:
		return chsql.Hex(chsql.Ident("trace_id")), true
	case logstorage.LabelSpanID:
		return chsql.Hex(chsql.Ident("span_id")), true
	case logstorage.LabelSeverity:
		return chsql.Ident("severity_text"), true
	case logstorage.LabelBody:
		return chsql.Ident("body"), true
	case logstorage.LabelServiceName, logstorage.LabelServiceNamespace, logstorage.LabelServiceInstanceID:
		return chsql.Ident(labelName), true
	default:
		return column, false
	}
}

// Series returns all available log series.
func (q *Querier) Series(ctx context.Context, opts logstorage.SeriesOptions) (result logstorage.Series, rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.Series",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			xattribute.StringerSlice("chstorage.selectors", opts.Selectors),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var materializedMap chsql.Expr
	{
		var (
			materialized = []string{
				logstorage.LabelTraceID,
				logstorage.LabelSpanID,
				logstorage.LabelSeverity,
				logstorage.LabelBody,
				logstorage.LabelServiceName,
				logstorage.LabelServiceInstanceID,
				logstorage.LabelServiceNamespace,
			}
			entries = make([]chsql.Expr, 0, len(materialized)*2)
		)
		for _, label := range materialized {
			expr, _ := q.getMaterializedLabelColumn(label)
			entries = append(entries, chsql.String(label), expr)
		}

		materializedMap = chsql.Map(entries...)
	}

	var (
		series = proto.NewMap(
			new(proto.ColStr),
			new(proto.ColStr),
		)

		query = chsql.Select(table, chsql.ResultColumn{
			Name: "series",
			Expr: chsql.MapConcat(
				materializedMap,
				attrStringMap(colAttrs),
				attrStringMap(colResource),
				attrStringMap(colScope),
			),
			Data: series,
		}).Where(
			chsql.InTimeRange("timestamp", opts.Start, opts.End),
		)
	)
	if sels := opts.Selectors; len(sels) > 0 {
		// Gather all labels for mapping fetch.
		labels := make([]string, 0, len(sels))
		for _, sel := range sels {
			for _, m := range sel.Matchers {
				labels = append(labels, string(m.Label))
			}
		}
		mapping, err := q.getLabelMapping(ctx, labels)
		if err != nil {
			return nil, errors.Wrap(err, "get label mapping")
		}

		sets := make([]chsql.Expr, 0, len(sels))
		for _, sel := range sels {
			selExprs := make([]chsql.Expr, 0, len(sel.Matchers))
			for _, m := range sel.Matchers {
				expr, err := q.logqlLabelMatcher(m, mapping)
				if err != nil {
					return result, err
				}
				selExprs = append(selExprs, expr)
			}
			sets = append(sets, chsql.JoinAnd(selExprs...))
		}
		if len(sets) > 0 {
			query.Where(chsql.JoinOr(sets...))
		}
	}

	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < series.Rows(); i++ {
				s := make(map[string]string)
				forEachColMap(series, i, func(k, v string) {
					s[otelstorage.KeyToLabel(k)] = v
				})
				result = append(result, s)
			}
			return nil
		},

		Type:   "Series",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return result, nil
}

func forEachColMap[K comparable, V any](c *proto.ColMap[K, V], row int, cb func(K, V)) {
	var start int
	end := int(c.Offsets[row])
	if row > 0 {
		start = int(c.Offsets[row-1])
	}
	for idx := start; idx < end; idx++ {
		cb(c.Keys.Row(idx), c.Values.Row(idx))
	}
}
