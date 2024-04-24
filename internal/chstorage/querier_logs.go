package chstorage

import (
	"context"
	"fmt"
	"slices"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

var (
	_ logstorage.Querier  = (*Querier)(nil)
	_ logqlengine.Querier = (*Querier)(nil)
)

// LabelNames implements logstorage.Querier.
func (q *Querier) LabelNames(ctx context.Context, opts logstorage.LabelsOptions) (_ []string, rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "LabelNames",
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
		out   []string
	)
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Result: proto.Results{
			{Name: "key", Data: &names},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < names.Rows(); i++ {
				name := names.Row(i)
				// TODO: add configuration option
				name = otelstorage.KeyToLabel(name)
				out = append(out, name)
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
		return nil, errors.Wrap(err, "select")
	}

	// Append materialized labels.
	out = append(out,
		logstorage.LabelTraceID,
		logstorage.LabelSpanID,
		logstorage.LabelSeverity,
		logstorage.LabelBody,
		logstorage.LabelServiceName,
		logstorage.LabelServiceInstanceID,
		logstorage.LabelServiceNamespace,
	)

	// Deduplicate.
	seen := make(map[string]struct{}, len(out))
	for _, v := range out {
		seen[v] = struct{}{}
	}
	out = out[:0]
	for k := range seen {
		out = append(out, k)
	}
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

	ctx, span := q.tracer.Start(ctx, "LabelValues",
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
		return nil, errors.Wrap(err, "select")
	}
	return &labelStaticIterator{
		name:   labelName,
		values: out,
	}, nil
}

func (q *Querier) getLabelMapping(ctx context.Context, labels []string) (_ map[string]string, rerr error) {
	ctx, span := q.tracer.Start(ctx, "getLabelMapping",
		trace.WithAttributes(
			attribute.Int("chstorage.labels_count", len(labels)),
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
		return nil, errors.Wrap(err, "select")
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
