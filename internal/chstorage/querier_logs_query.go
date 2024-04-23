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

// LogsQuery defines a logs query.
type LogsQuery[E any] struct {
	Start, End time.Time
	Direction  logqlengine.Direction

	Labels []logql.LabelMatcher
	Line   []logql.LineFilter

	Mapper func(logstorage.Record) (E, error)
}

// Eval evaluates the query using given querier.
func (q *LogsQuery[E]) Eval(ctx context.Context, querier *Querier) (_ iterators.Iterator[E], rerr error) {
	table := querier.tables.Logs

	ctx, span := querier.tracer.Start(ctx, "LogsQuery",
		trace.WithAttributes(
			attribute.Int("chstorage.labels_count", len(q.Labels)),
			attribute.Int64("chstorage.range.start", q.Start.UnixNano()),
			attribute.Int64("chstorage.range.end", q.End.UnixNano()),
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
	for _, m := range q.Labels {
		labels = append(labels, string(m.Label))
	}
	mapping, err := querier.getLabelMapping(ctx, labels)
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
	fmt.Fprintf(&query, " FROM %s WHERE (toUnixTimestamp64Nano(timestamp) >= %d AND toUnixTimestamp64Nano(timestamp) <= %d)",
		table, q.Start.UnixNano(), q.End.UnixNano())
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

	for _, m := range q.Line {
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
	switch d := q.Direction; d {
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
	if err := querier.ch.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query.String(),
		Result: out.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			if err := out.ForEach(func(r logstorage.Record) error {
				e, err := q.Mapper(r)
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

	querier.clickhouseRequestHistogram.Record(ctx, time.Since(queryStartTime).Seconds(),
		metric.WithAttributes(
			attribute.String("chstorage.table", table),
			attribute.String("chstorage.signal", "logs"),
		),
	)
	return iterators.Slice(data), nil
}
