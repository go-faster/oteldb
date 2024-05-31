package chstorage

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"

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
			attribute.Int64("chstorage.range.start", int64(opts.Start)),
			attribute.Int64("chstorage.range.end", int64(opts.End)),
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
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Result: proto.Results{
			{Name: "key", Data: &names},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < names.Rows(); i++ {
				// TODO: add configuration option
				name := otelstorage.KeyToLabel(names.Row(i))
				dedup[name] = struct{}{}
			}
			return nil
		},
		Body: fmt.Sprintf(`SELECT DISTINCT
arrayJoin(arrayConcat(%s, %s, %s)) as key
FROM %s
WHERE (toUnixTimestamp64Nano(timestamp) >= %d AND toUnixTimestamp64Nano(timestamp) <= %d)
LIMIT 1000`,
			attrKeys(colAttrs),
			attrKeys(colResource),
			attrKeys(colScope),
			table, opts.Start, opts.End,
		),
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
			attribute.Int64("chstorage.range.start", int64(opts.Start)),
			attribute.Int64("chstorage.range.end", int64(opts.End)),
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
	var out []string
	values := new(proto.ColStr).Array()
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Result: proto.Results{
			{Name: "values", Data: values},
		},
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
		Body: fmt.Sprintf(`SELECT DISTINCT
array(
	%s,
	%s,
	%s
) as values
FROM %s
WHERE (toUnixTimestamp64Nano(timestamp) >= %d AND toUnixTimestamp64Nano(timestamp) <= %d) LIMIT 1000`,
			attrSelector(colAttrs, labelName),
			attrSelector(colResource, labelName),
			attrSelector(colScope, labelName),
			table, opts.Start, opts.End,
		),
	}); err != nil {
		return nil, err
	}

	return &labelStaticIterator{
		name:   labelName,
		values: out,
	}, nil
}

func (q *Querier) getLabelMapping(ctx context.Context, labels []string) (_ map[string]string, rerr error) {
	ctx, span := q.tracer.Start(ctx, "chstorage.logs.getLabelMapping",
		trace.WithAttributes(
			attribute.StringSlice("chstorage.labels", labels),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	out := make(map[string]string, len(labels))
	attrs := newLogAttrMapColumns()
	var inputData proto.ColStr
	for _, label := range labels {
		inputData.Append(label)
	}
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Result: attrs.Result(),
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
		Body: fmt.Sprintf(`SELECT name, key FROM %[1]s WHERE name IN labels`, q.tables.LogAttrs),
	}); err != nil {
		return nil, err
	}

	return out, nil
}

func (q *Querier) getMaterializedLabelColumn(labelName string) (column string, isColumn bool) {
	switch labelName {
	case logstorage.LabelTraceID:
		return "hex(trace_id)", true
	case logstorage.LabelSpanID:
		return "hex(span_id)", true
	case logstorage.LabelSeverity:
		return "severity_number", true
	case logstorage.LabelBody:
		return "body", true
	case logstorage.LabelServiceName, logstorage.LabelServiceNamespace, logstorage.LabelServiceInstanceID:
		return labelName, true
	default:
		return "", false
	}
}

// Series returns all available log series.
func (q *Querier) Series(ctx context.Context, opts logstorage.SeriesOptions) (result logstorage.Series, rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.Series",
		trace.WithAttributes(
			attribute.Int64("chstorage.range.start", opts.Start.UnixNano()),
			attribute.Int64("chstorage.range.end", opts.Start.UnixNano()),
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

	var materializedStringMap strings.Builder
	{
		materializedStringMap.WriteString("map(")
		for i, label := range []string{
			logstorage.LabelTraceID,
			logstorage.LabelSpanID,
			logstorage.LabelSeverity,
			logstorage.LabelBody,
			logstorage.LabelServiceName,
			logstorage.LabelServiceInstanceID,
			logstorage.LabelServiceNamespace,
		} {
			if i != 0 {
				materializedStringMap.WriteByte(',')
			}
			materializedStringMap.WriteString(singleQuoted(label))
			materializedStringMap.WriteByte(',')
			expr, _ := q.getMaterializedLabelColumn(label)
			if label == logstorage.LabelSeverity {
				expr = fmt.Sprintf("toString(%s)", expr)
			}
			materializedStringMap.WriteString(expr)
		}
		materializedStringMap.WriteByte(')')
	}

	var query strings.Builder
	fmt.Fprintf(&query, `SELECT DISTINCT
	mapConcat(%s, %s, %s, %s) as series
FROM %s
WHERE (toUnixTimestamp64Nano(timestamp) >= %d AND toUnixTimestamp64Nano(timestamp) <= %d)`,
		&materializedStringMap,
		attrStringMap(colAttrs),
		attrStringMap(colResource),
		attrStringMap(colScope),
		table,
		opts.Start.UnixNano(), opts.End.UnixNano(),
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

		query.WriteString(" AND (false")
		for _, sel := range sels {
			query.WriteString(" OR (true")
			if err := writeLabelMatchers(&query, sel.Matchers, mapping); err != nil {
				return nil, err
			}
			query.WriteByte(')')
		}
		query.WriteByte(')')
	}
	query.WriteString(" LIMIT 1000")

	var (
		queryStartTime = time.Now()

		series = proto.NewMap(
			new(proto.ColStr),
			new(proto.ColStr),
		)
	)
	if err := q.ch.Do(ctx, ch.Query{
		Body: query.String(),
		Result: proto.Results{
			{Name: "series", Data: series},
		},
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
	}); err != nil {
		return nil, err
	}

	q.clickhouseRequestHistogram.Record(ctx, time.Since(queryStartTime).Seconds(),
		metric.WithAttributes(
			attribute.String("chstorage.query_type", "Series"),
			attribute.String("chstorage.table", table),
			attribute.String("chstorage.signal", "logs"),
		),
	)
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
