package chstorage

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/xattribute"
)

// LogsQuery defines a logs query.
type LogsQuery[E any] struct {
	Start, End time.Time
	Direction  logqlengine.Direction
	Limit      int

	Labels []logql.LabelMatcher
	Line   []logql.LineFilter

	Mapper func(logstorage.Record) (E, error)
}

// Execute executes the query using given querier.
func (v *LogsQuery[E]) Execute(ctx context.Context, q *Querier) (_ iterators.Iterator[E], rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.LogsQuery.Eval",
		trace.WithAttributes(
			xattribute.UnixNano("logql.range.start", v.Start),
			xattribute.UnixNano("logql.range.end", v.End),
			attribute.Stringer("logql.direction", v.Direction),
			attribute.Int("logql.limit", v.Limit),
			xattribute.StringerSlice("logql.label_matchers", v.Labels),
			xattribute.StringerSlice("logql.line_matchers", v.Line),

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
	labels := make([]string, 0, len(v.Labels))
	for _, m := range v.Labels {
		labels = append(labels, string(m.Label))
	}
	mapping, err := q.getLabelMapping(ctx, labels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	var (
		out   = newLogColumns()
		query = chsql.Select(table, out.ChsqlResult()...)
	)
	if err := (logQueryPredicates{
		Start:  v.Start,
		End:    v.End,
		Labels: v.Labels,
		Line:   v.Line,
	}).write(query, mapping, q); err != nil {
		return nil, err
	}

	switch d := v.Direction; d {
	case logqlengine.DirectionBackward:
		query.Order(chsql.Ident("timestamp"), chsql.Desc)
	case logqlengine.DirectionForward:
		query.Order(chsql.Ident("timestamp"), chsql.Asc)
	default:
		return nil, errors.Errorf("unexpected direction %q", d)
	}
	query.Limit(v.Limit)

	var data []E
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			return out.ForEach(func(r logstorage.Record) error {
				e, err := v.Mapper(r)
				if err != nil {
					return err
				}
				data = append(data, e)
				return nil
			})
		},

		Type:   "QueryLogs",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}

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

// Execute executes the query using given querier.
func (v *SampleQuery) Execute(ctx context.Context, q *Querier) (_ logqlengine.SampleIterator, rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.SampleQuery.Eval",
		trace.WithAttributes(
			xattribute.UnixNano("logql.range.start", v.Start),
			xattribute.UnixNano("logql.range.end", v.End),
			attribute.String("logql.sampling", v.Sampling.String()),
			xattribute.StringerSlice("logql.grouping_labels", v.GroupingLabels),
			xattribute.StringerSlice("logql.label_matchers", v.Labels),
			xattribute.StringerSlice("logql.line_matchers", v.Line),

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
	labels := make([]string, 0, len(v.Labels)+len(v.GroupingLabels))
	for _, m := range v.Labels {
		labels = append(labels, string(m.Label))
	}
	for _, l := range v.GroupingLabels {
		labels = append(labels, string(l))
	}
	mapping, err := q.getLabelMapping(ctx, labels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	sampleExpr, err := getSampleExpr(v.Sampling)
	if err != nil {
		return nil, err
	}

	entries := make([]chsql.Expr, 0, len(v.GroupingLabels)*2)
	for _, key := range v.GroupingLabels {
		label := string(key)
		if key, ok := mapping[label]; ok {
			label = key
		}

		labelExpr, ok := q.getMaterializedLabelColumn(label)
		if !ok {
			labelExpr = firstAttrSelector(label)
		}

		entries = append(entries,
			chsql.String(string(key)),
			chsql.ToString(labelExpr),
		)
	}

	var (
		columns = sampleQueryColumns{
			Timestamp: proto.ColDateTime64{},
			Sample:    proto.ColFloat64{},
			Labels: proto.NewMap(
				new(proto.ColStr),
				new(proto.ColStr),
			),
		}

		query = chsql.Select(table,
			chsql.Column("timestamp", &columns.Timestamp),
			chsql.ResultColumn{
				Name: "sample",
				Expr: chsql.ToFloat64(sampleExpr),
				Data: &columns.Sample,
			},
			chsql.ResultColumn{
				Name: "labels",
				Expr: chsql.Map(entries...),
				Data: columns.Labels,
			},
		)
	)
	if err := (logQueryPredicates{
		Start:  v.Start,
		End:    v.End,
		Labels: v.Labels,
		Line:   v.Line,
	}).write(query, mapping, q); err != nil {
		return nil, err
	}
	query.Order(chsql.Ident("timestamp"), chsql.Asc)

	var result []logqlmetric.SampledEntry
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < columns.Timestamp.Rows(); i++ {
				timestamp := columns.Timestamp.Row(i)
				sample := columns.Sample.Row(i)
				// FIXME(tdakkota): allocates unnecessary map.
				labels := columns.Labels.Row(i)

				result = append(result, logqlmetric.SampledEntry{
					Timestamp: pcommon.NewTimestampFromTime(timestamp),
					Sample:    sample,
					Set:       logqlabels.AggregatedLabelsFromMap(labels),
				})
			}
			return nil
		},

		Type:   "QuerySamples",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
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

func getSampleExpr(op SamplingOp) (chsql.Expr, error) {
	switch op {
	case CountSampling:
		return chsql.Integer(1), nil
	case BytesSampling:
		return chsql.Length(chsql.Ident("body")), nil
	default:
		return chsql.Expr{}, errors.Errorf("unexpected sampling op: %v", op)
	}
}

// logQueryPredicates translates common predicates for log querying.
type logQueryPredicates struct {
	Start, End time.Time
	Labels     []logql.LabelMatcher
	Line       []logql.LineFilter
}

func (p logQueryPredicates) write(
	query *chsql.SelectQuery,
	mapping map[string]string,
	q *Querier,
) error {
	query.Where(
		chsql.InTimeRange("timestamp", p.Start, p.End),
	)
	for _, m := range p.Labels {
		expr, err := q.logQLLabelMatcher(m, mapping)
		if err != nil {
			return err
		}
		query.Where(expr)
	}
	for _, m := range p.Line {
		expr, err := q.lineFilter(m)
		if err != nil {
			return err
		}
		query.Where(expr)
	}
	return nil
}

func (q *Querier) lineFilter(m logql.LineFilter) (e chsql.Expr, rerr error) {
	defer func() {
		if rerr == nil {
			switch m.Op {
			case logql.OpNotEq, logql.OpNotRe:
				e = chsql.Not(e)
			}
		}
	}()

	switch m.Op {
	case logql.OpEq, logql.OpNotEq:
		expr := chsql.Contains("body", m.By.Value)

		// Clickhouse does not use tokenbf_v1 index to skip blocks
		// with position* functions for some reason.
		//
		// Force to skip using hasToken function.
		//
		// Note that such optimization is applied only if operation is not negated to
		// avoid false-negative skipping.
		if val := m.By.Value; m.Op != logql.OpNotEq && chsql.IsSingleToken(val) {
			expr = chsql.And(expr,
				chsql.HasToken(chsql.Ident("body"), val),
			)
		}

		{
			// HACK: check for special case of hex-encoded trace_id and span_id.
			// Like `{http_method=~".+"} |= "af36000000000000c517000000000003"`.
			// TODO(ernado): also handle regex?
			v, _ := hex.DecodeString(m.By.Value)
			switch len(v) {
			case len(otelstorage.TraceID{}):
				expr = chsql.Or(expr, chsql.Eq(
					chsql.Ident("trace_id"),
					chsql.Unhex(chsql.String(m.By.Value)),
				))
			case len(otelstorage.SpanID{}):
				expr = chsql.Or(expr, chsql.Eq(
					chsql.Ident("span_id"),
					chsql.Unhex(chsql.String(m.By.Value)),
				))
			}
		}
		return expr, nil
	case logql.OpRe, logql.OpNotRe:
		return chsql.Match(chsql.Ident("body"), chsql.String(m.By.Value)), nil
	default:
		return e, errors.Errorf("unexpected line matcher op %v", m.Op)
	}
}

func (q *Querier) logQLLabelMatcher(
	m logql.LabelMatcher,
	mapping map[string]string,
) (e chsql.Expr, rerr error) {
	defer func() {
		if rerr == nil {
			switch m.Op {
			case logql.OpNotEq, logql.OpNotRe:
				e = chsql.Not(e)
			}
		}
	}()

	matchHex := func(column chsql.Expr, m logql.LabelMatcher) (e chsql.Expr, _ error) {
		switch m.Op {
		case logql.OpEq, logql.OpNotEq:
			return chsql.Eq(
				column,
				chsql.Unhex(chsql.String(m.Value)),
			), nil
		case logql.OpRe, logql.OpNotRe:
			// FIXME(tdakkota): match is case-sensitive
			return chsql.Match(
				chsql.Hex(column),
				chsql.String(m.Value),
			), nil
		default:
			return e, errors.Errorf("unexpected label matcher op %v", m.Op)
		}
	}

	labelName := string(m.Label)
	if key, ok := mapping[labelName]; ok {
		labelName = key
	}

	switch labelName {
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
			return chsql.ColumnEq("severity_number", severityNumber), nil
		case logql.OpRe, logql.OpNotRe:
			matches := make([]int, 0, 6)
			for i := plog.SeverityNumberUnspecified; i <= plog.SeverityNumberFatal4; i++ {
				for _, s := range []string{
					i.String(),
					strings.ToLower(i.String()),
					strings.ToUpper(i.String()),
				} {
					if m.Re.MatchString(s) {
						matches = append(matches, int(i))
						break
					}
				}
			}
			return chsql.In(chsql.Ident("severity_number"), chsql.TupleValues(matches...)), nil
		default:
			return e, errors.Errorf("unexpected label matcher op %v", m.Op)
		}
	case logstorage.LabelBody:
		switch m.Op {
		case logql.OpEq, logql.OpNotEq:
			return chsql.Contains("body", m.Value), nil
		case logql.OpRe, logql.OpNotRe:
			return chsql.Match(chsql.Ident("body"), chsql.String(m.Value)), nil
		default:
			return e, errors.Errorf("unexpected label matcher op %v", m.Op)
		}
	case logstorage.LabelSpanID:
		return matchHex(chsql.Ident("span_id"), m)
	case logstorage.LabelTraceID:
		return matchHex(chsql.Ident("trace_id"), m)
	default:
		expr, ok := q.getMaterializedLabelColumn(labelName)
		if ok {
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				return chsql.Eq(expr, chsql.String(m.Value)), nil
			case logql.OpRe, logql.OpNotRe:
				return chsql.Match(expr, chsql.String(m.Value)), nil
			default:
				return e, errors.Errorf("unexpected label matcher op %v", m.Op)
			}
		}

		exprs := make([]chsql.Expr, 0, 3)
		// Search in all attributes.
		for _, column := range []string{
			colAttrs,
			colResource,
			colScope,
		} {
			// TODO: how to match integers, booleans, floats, arrays?
			var (
				selector = attrSelector(column, labelName)
				sub      chsql.Expr
			)
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				sub = chsql.Eq(selector, chsql.String(m.Value))
			case logql.OpRe, logql.OpNotRe:
				sub = chsql.Match(selector, chsql.String(m.Value))
			default:
				return e, errors.Errorf("unexpected label matcher op %v", m.Op)
			}
			exprs = append(exprs, sub)
		}
		return chsql.JoinOr(exprs...), nil
	}
}
