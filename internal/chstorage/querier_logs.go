package chstorage

import (
	"context"
	"encoding/hex"
	"fmt"
	"regexp"
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

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
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

// Capabilities implements logqlengine.Querier.
func (q *Querier) Capabilities() (caps logqlengine.QuerierCapabilities) {
	caps.Label.Add(logql.OpEq, logql.OpNotEq, logql.OpRe, logql.OpNotRe)
	caps.Line.Add(logql.OpEq, logql.OpNotEq, logql.OpRe, logql.OpNotRe)
	return caps
}

type logStaticIterator struct {
	data []logstorage.Record
}

func (l *logStaticIterator) Next(t *logstorage.Record) bool {
	if len(l.data) == 0 {
		return false
	}
	*t = l.data[0]
	l.data = l.data[1:]
	return true
}

func (l *logStaticIterator) Err() error   { return nil }
func (l *logStaticIterator) Close() error { return nil }

// SelectLogs implements logqlengine.Querier.
func (q *Querier) SelectLogs(ctx context.Context, start, end otelstorage.Timestamp, direction logqlengine.Direction, params logqlengine.SelectLogsParams) (_ iterators.Iterator[logstorage.Record], rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "SelectLogs",
		trace.WithAttributes(
			attribute.Int("chstorage.labels_count", len(params.Labels)),
			attribute.Int64("chstorage.range.start", int64(start)),
			attribute.Int64("chstorage.range.end", int64(end)),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	// Gather all labels for mapping fetch.
	var labels []string
	for _, m := range params.Labels {
		labels = append(labels, string(m.Label))
	}
	mapping, err := q.getLabelMapping(ctx, labels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	out := newLogColumns()
	var query strings.Builder
	query.WriteString("SELECT ")
	for i, column := range out.StaticColumns() {
		if i != 0 {
			query.WriteByte(',')
		}
		query.WriteString(column)
	}
	fmt.Fprintf(&query, " FROM %s WHERE (toUnixTimestamp64Nano(timestamp) >= %d AND toUnixTimestamp64Nano(timestamp) <= %d)", table, start, end)
	for _, m := range params.Labels {
		labelName := string(m.Label)
		if key, ok := mapping[labelName]; ok {
			labelName = key
		}
		switch m.Op {
		case logql.OpEq, logql.OpRe:
			query.WriteString(" AND (")
		case logql.OpNotEq, logql.OpNotRe:
			query.WriteString(" AND NOT (")
		default:
			return nil, errors.Errorf("unexpected op %q", m.Op)
		}
		switch labelName {
		case logstorage.LabelTraceID:
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				fmt.Fprintf(&query, "trace_id = unhex(%s)", singleQuoted(m.Value))
			case logql.OpRe, logql.OpNotRe:
				fmt.Fprintf(&query, "match(hex(trace_id), %s)", singleQuoted(m.Value))
			default:
				return nil, errors.Errorf("unexpected op %q", m.Op)
			}
		case logstorage.LabelSpanID:
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				fmt.Fprintf(&query, "span_id = unhex(%s)", singleQuoted(m.Value))
			case logql.OpRe, logql.OpNotRe:
				fmt.Fprintf(&query, "match(hex(span_id), %s)", singleQuoted(m.Value))
			default:
				return nil, errors.Errorf("unexpected op %q", m.Op)
			}
		case logstorage.LabelSeverity:
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				// Direct comparison with severity number.
				var severityNumber uint8
				for i := plog.SeverityNumberUnspecified; i <= plog.SeverityNumberFatal4; i++ {
					if strings.EqualFold(i.String(), m.Value) {
						severityNumber = uint8(i)
						break
					}
				}
				fmt.Fprintf(&query, "severity_number = %d", severityNumber)
			case logql.OpRe, logql.OpNotRe:
				re, err := regexp.Compile(m.Value)
				if err != nil {
					return nil, errors.Wrap(err, "compile regex")
				}
				var matches []int
				for i := plog.SeverityNumberUnspecified; i <= plog.SeverityNumberFatal4; i++ {
					for _, s := range []string{
						i.String(),
						strings.ToLower(i.String()),
						strings.ToUpper(i.String()),
					} {
						if re.MatchString(s) {
							matches = append(matches, int(i))
							break
						}
					}
				}
				query.WriteString("severity_number IN (")
				for i, v := range matches {
					if i != 0 {
						query.WriteByte(',')
					}
					fmt.Fprintf(&query, "%d", v)
				}
				query.WriteByte(')')
			default:
				return nil, errors.Errorf("unexpected op %q", m.Op)
			}
		case logstorage.LabelBody:
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				fmt.Fprintf(&query, "positionUTF8(body, %s) > 0", singleQuoted(m.Value))
			case logql.OpRe, logql.OpNotRe:
				fmt.Fprintf(&query, "match(body, %s)", singleQuoted(m.Value))
			default:
				return nil, errors.Errorf("unexpected op %q", m.Op)
			}
		case logstorage.LabelServiceName, logstorage.LabelServiceNamespace, logstorage.LabelServiceInstanceID:
			// Materialized from resource.service.{name,namespace,instance.id}.
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				fmt.Fprintf(&query, "%s = %s", labelName, singleQuoted(m.Value))
			case logql.OpRe, logql.OpNotRe:
				fmt.Fprintf(&query, "match(%s, %s)", labelName, singleQuoted(m.Value))
			default:
				return nil, errors.Errorf("unexpected op %q", m.Op)
			}
		default:
			// Search in all attributes.
			for i, column := range []string{
				colAttrs,
				colResource,
				colScope,
			} {
				if i != 0 {
					query.WriteString(" OR ")
				}
				// TODO: how to match integers, booleans, floats, arrays?

				selector := attrSelector(column, labelName)
				switch m.Op {
				case logql.OpEq, logql.OpNotEq:
					fmt.Fprintf(&query, "%s = %s", selector, singleQuoted(m.Value))
				case logql.OpRe, logql.OpNotRe:
					fmt.Fprintf(&query, "match(%s, %s)", selector, singleQuoted(m.Value))
				default:
					return nil, errors.Errorf("unexpected op %q", m.Op)
				}
			}
		}
		query.WriteByte(')')
	}

	for _, m := range params.Line {
		switch m.Op {
		case logql.OpEq, logql.OpRe:
			query.WriteString(" AND (")
		case logql.OpNotEq, logql.OpNotRe:
			query.WriteString(" AND NOT (")
		default:
			return nil, errors.Errorf("unexpected op %q", m.Op)
		}

		switch m.Op {
		case logql.OpEq, logql.OpNotEq:
			fmt.Fprintf(&query, "positionUTF8(body, %s) > 0", singleQuoted(m.Value))
			{
				// HACK: check for special case of hex-encoded trace_id and span_id.
				// Like `{http_method=~".+"} |= "af36000000000000c517000000000003"`.
				// TODO(ernado): also handle regex?
				encoded := strings.ToLower(m.Value)
				v, _ := hex.DecodeString(encoded)
				switch len(v) {
				case len(otelstorage.TraceID{}):
					fmt.Fprintf(&query, " OR trace_id = unhex(%s)", singleQuoted(encoded))
				case len(otelstorage.SpanID{}):
					fmt.Fprintf(&query, " OR span_id = unhex(%s)", singleQuoted(encoded))
				}
			}
		case logql.OpRe, logql.OpNotRe:
			fmt.Fprintf(&query, "match(body, %s)", singleQuoted(m.Value))
		}
		query.WriteByte(')')
	}

	query.WriteString(" ORDER BY timestamp ")
	switch direction {
	case logqlengine.DirectionBackward:
		query.WriteString("DESC")
	case logqlengine.DirectionForward:
		query.WriteString("ASC")
	default:
		return nil, errors.Errorf("unexpected direction %q", direction)
	}

	var data []logstorage.Record
	queryStartTime := time.Now()
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query.String(),
		Result: out.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			if err := out.ForEach(func(r logstorage.Record) {
				data = append(data, r)
			}); err != nil {
				return errors.Wrap(err, "for each")
			}
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "select")
	}
	q.clickhouseRequestHistogram.Record(ctx, time.Since(queryStartTime).Seconds(),
		metric.WithAttributes(
			attribute.String("chstorage.table", table),
			attribute.String("chstorage.signal", "logs"),
		),
	)

	return &logStaticIterator{data: data}, nil
}
