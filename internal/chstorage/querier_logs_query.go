package chstorage

import (
	"context"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// LogsQuery defines a logs query.
type LogsQuery[E any] struct {
	Start, End time.Time
	Direction  logqlengine.Direction

	Labels []logql.LabelMatcher
	Line   []logql.LineFilter

	Mapper func(logstorage.Record) (E, error)
}

// Eval evaluates the query using given querier.
func (v *LogsQuery[E]) Eval(ctx context.Context, q *Querier) (_ iterators.Iterator[E], rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "LogsQuery",
		trace.WithAttributes(
			attribute.Int64("chstorage.range.start", v.Start.UnixNano()),
			attribute.Int64("chstorage.range.end", v.End.UnixNano()),
			attribute.String("chstorage.direction", string(v.Direction)),
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
	for _, m := range v.Labels {
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
	fmt.Fprintf(&query, "\nFROM %s WHERE\n", table)

	if err := (logQueryPredicates{
		Start:  v.Start,
		End:    v.End,
		Labels: v.Labels,
		Line:   v.Line,
	}).write(&query, mapping); err != nil {
		return nil, err
	}

	query.WriteString(" ORDER BY timestamp ")
	switch d := v.Direction; d {
	case logqlengine.DirectionBackward:
		query.WriteString("DESC")
	case logqlengine.DirectionForward:
		query.WriteString("ASC")
	default:
		return nil, errors.Errorf("unexpected direction %q", d)
	}

	var (
		data           []E
		queryStartTime = time.Now()
	)
	if err := q.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query.String(),
		Result: out.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			if err := out.ForEach(func(r logstorage.Record) error {
				e, err := v.Mapper(r)
				if err != nil {
					return err
				}
				data = append(data, e)
				return nil
			}); err != nil {
				return errors.Wrap(err, "for each")
			}
			return rerr
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
	return iterators.Slice(data), nil
}

// SampleQuery defines a sample query.
type SampleQuery struct {
	Start, End time.Time

	Sampling       SamplingOp
	GroupingLabels []logql.Label

	Labels []logql.LabelMatcher
	Line   []logql.LineFilter
}

// sampleQueryColumns defines result columns of [SampleQuery].
type sampleQueryColumns struct {
	Timestamp proto.ColDateTime64
	Sample    proto.ColFloat64
	Labels    *proto.ColMap[string, string]
}

func (c *sampleQueryColumns) Result() proto.Results {
	return proto.Results{
		{Name: "timestamp", Data: &c.Timestamp},
		{Name: "sample", Data: &c.Sample},
		{Name: "labels", Data: c.Labels},
	}
}

// Eval evaluates the query using given querier.
func (v *SampleQuery) Eval(ctx context.Context, q *Querier) (_ logqlengine.SampleIterator, rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "LogsQuery",
		trace.WithAttributes(
			attribute.Int64("chstorage.range.start", v.Start.UnixNano()),
			attribute.Int64("chstorage.range.end", v.End.UnixNano()),
			attribute.String("chstorage.direction", v.Sampling.String()),
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
	for _, m := range v.Labels {
		labels = append(labels, string(m.Label))
	}
	mapping, err := q.getLabelMapping(ctx, labels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	var query strings.Builder
	sampleExpr, err := getSampleExpr(v.Sampling)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(&query, "SELECT timestamp, toFloat64(%s) AS sample, map(\n", sampleExpr)
	for i, label := range v.GroupingLabels {
		quotedLabel := singleQuoted(string(label))

		labelExpr, ok := q.getMaterializedLabelColumn(label)
		if !ok {
			labelExpr = firstAttrSelector(string(label))
		}

		if i != 0 {
			query.WriteString(",\n")
		}
		fmt.Fprintf(&query, "%s, toString(%s)", quotedLabel, labelExpr)
	}
	query.WriteString("\n) AS labels\n")

	fmt.Fprintf(&query, "FROM %s WHERE\n", table)
	pred := logQueryPredicates{
		Start:  v.Start,
		End:    v.End,
		Labels: v.Labels,
		Line:   v.Line,
	}
	if err := pred.write(&query, mapping); err != nil {
		return nil, err
	}

	query.WriteString(`ORDER BY timestamp ASC;`)

	var (
		result []logqlmetric.SampledEntry

		columns = sampleQueryColumns{
			Timestamp: proto.ColDateTime64{},
			Sample:    proto.ColFloat64{},
			Labels: proto.NewMap(
				new(proto.ColStr),
				new(proto.ColStr),
			),
		}
	)
	if err := q.ch.Do(ctx, ch.Query{
		Body:   query.String(),
		Result: columns.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < columns.Timestamp.Rows(); i++ {
				timestamp := columns.Timestamp.Row(i)
				sample := columns.Sample.Row(i)
				// FIXME(tdakkota): allocates unnecessary map.
				labels := columns.Labels.Row(i)

				result = append(result, logqlmetric.SampledEntry{
					Timestamp: pcommon.NewTimestampFromTime(timestamp),
					Sample:    sample,
					Set:       logqlengine.AggregatedLabelsFromMap(labels),
				})
			}
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "clickhouse")
	}

	return iterators.Slice(result), nil
}

// SamplingOp defines a sampler operation.
type SamplingOp int

const (
	// CountSampling counts lines.
	CountSampling SamplingOp = iota + 1
	// BytesSampling counts line lengths in bytes.
	BytesSampling
)

// String implments [fmt.Stringer].
func (s SamplingOp) String() string {
	switch s {
	case CountSampling:
		return "count"
	case BytesSampling:
		return "bytes"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

func getSampleExpr(op SamplingOp) (string, error) {
	switch op {
	case CountSampling:
		return "1", nil
	case BytesSampling:
		return "length(message)", nil
	default:
		return "", errors.Errorf("unexpected sampling op: %v", op)
	}
}

// logQueryPredicates translates common predicates for log querying.
type logQueryPredicates struct {
	Start, End time.Time
	Labels     []logql.LabelMatcher
	Line       []logql.LineFilter
}

func (q logQueryPredicates) write(
	query *strings.Builder,
	mapping map[string]string,
) error {
	fmt.Fprintf(query, "(toUnixTimestamp64Nano(timestamp) >= %d AND toUnixTimestamp64Nano(timestamp) <= %d)",
		q.Start.UnixNano(), q.End.UnixNano())
	for _, m := range q.Labels {
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
			return errors.Errorf("unexpected op %q", m.Op)
		}
		switch labelName {
		case logstorage.LabelTraceID:
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				fmt.Fprintf(query, "trace_id = unhex(%s)", singleQuoted(m.Value))
			case logql.OpRe, logql.OpNotRe:
				fmt.Fprintf(query, "match(hex(trace_id), %s)", singleQuoted(m.Value))
			default:
				return errors.Errorf("unexpected op %q", m.Op)
			}
		case logstorage.LabelSpanID:
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				fmt.Fprintf(query, "span_id = unhex(%s)", singleQuoted(m.Value))
			case logql.OpRe, logql.OpNotRe:
				fmt.Fprintf(query, "match(hex(span_id), %s)", singleQuoted(m.Value))
			default:
				return errors.Errorf("unexpected op %q", m.Op)
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
				fmt.Fprintf(query, "severity_number = %d", severityNumber)
			case logql.OpRe, logql.OpNotRe:
				re, err := regexp.Compile(m.Value)
				if err != nil {
					return errors.Wrap(err, "compile regex")
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
					fmt.Fprintf(query, "%d", v)
				}
				query.WriteByte(')')
			default:
				return errors.Errorf("unexpected op %q", m.Op)
			}
		case logstorage.LabelBody:
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				fmt.Fprintf(query, "positionUTF8(body, %s) > 0", singleQuoted(m.Value))
			case logql.OpRe, logql.OpNotRe:
				fmt.Fprintf(query, "match(body, %s)", singleQuoted(m.Value))
			default:
				return errors.Errorf("unexpected op %q", m.Op)
			}
		case logstorage.LabelServiceName, logstorage.LabelServiceNamespace, logstorage.LabelServiceInstanceID:
			// Materialized from resource.service.{name,namespace,instance.id}.
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				fmt.Fprintf(query, "%s = %s", labelName, singleQuoted(m.Value))
			case logql.OpRe, logql.OpNotRe:
				fmt.Fprintf(query, "match(%s, %s)", labelName, singleQuoted(m.Value))
			default:
				return errors.Errorf("unexpected op %q", m.Op)
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
					fmt.Fprintf(query, "%s = %s", selector, singleQuoted(m.Value))
				case logql.OpRe, logql.OpNotRe:
					fmt.Fprintf(query, "match(%s, %s)", selector, singleQuoted(m.Value))
				default:
					return errors.Errorf("unexpected op %q", m.Op)
				}
			}
		}
		query.WriteByte(')')
	}

	for _, m := range q.Line {
		switch m.Op {
		case logql.OpEq, logql.OpRe:
			query.WriteString(" AND (")
		case logql.OpNotEq, logql.OpNotRe:
			query.WriteString(" AND NOT (")
		default:
			return errors.Errorf("unexpected op %q", m.Op)
		}

		switch m.Op {
		case logql.OpEq, logql.OpNotEq:
			fmt.Fprintf(query, "positionUTF8(body, %s) > 0", singleQuoted(m.Value))
			{
				// HACK: check for special case of hex-encoded trace_id and span_id.
				// Like `{http_method=~".+"} |= "af36000000000000c517000000000003"`.
				// TODO(ernado): also handle regex?
				encoded := strings.ToLower(m.Value)
				v, _ := hex.DecodeString(encoded)
				switch len(v) {
				case len(otelstorage.TraceID{}):
					fmt.Fprintf(query, " OR trace_id = unhex(%s)", singleQuoted(encoded))
				case len(otelstorage.SpanID{}):
					fmt.Fprintf(query, " OR span_id = unhex(%s)", singleQuoted(encoded))
				}
			}
		case logql.OpRe, logql.OpNotRe:
			fmt.Fprintf(query, "match(body, %s)", singleQuoted(m.Value))
		}
		query.WriteByte(')')
	}
	return nil
}
