package chstorage

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*Querier)(nil)

// Querier implements tracestorage.Querier using Clickhouse.
type Querier struct {
	ch     *chpool.Pool
	tables Tables
	tracer trace.Tracer
}

// QuerierOptions is Querier's options.
type QuerierOptions struct {
	// Tables provides table paths to query.
	Tables Tables
	// MeterProvider provides OpenTelemetry meter for this querier.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer for this querier.
	TracerProvider trace.TracerProvider
}

func (opts *QuerierOptions) setDefaults() {
	if opts.Tables == (Tables{}) {
		opts.Tables = defaultTables
	}
	if opts.MeterProvider == nil {
		opts.MeterProvider = otel.GetMeterProvider()
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}
}

// NewQuerier creates new Querier.
func NewQuerier(c *chpool.Pool, opts QuerierOptions) (*Querier, error) {
	opts.setDefaults()

	return &Querier{
		ch:     c,
		tables: opts.Tables,
		tracer: otel.Tracer("chstorage.Querier"),
	}, nil
}

// SearchTags performs search by given tags.
func (q *Querier) SearchTags(ctx context.Context, tags map[string]string, opts tracestorage.SearchTagsOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "TagNames",
		trace.WithAttributes(
			attribute.Int("chstorage.tags_count", len(tags)),
			attribute.Int64("chstorage.start_range", int64(opts.Start)),
			attribute.Int64("chstorage.end_range", int64(opts.End)),
			attribute.Int64("chstorage.max_duration", int64(opts.MaxDuration)),
			attribute.Int64("chstorage.min_duration", int64(opts.MinDuration)),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var query strings.Builder
	fmt.Fprintf(&query, `SELECT * FROM %#[1]q WHERE trace_id IN (
		SELECT trace_id FROM %#[1]q WHERE true
	`, table)
	for key, value := range tags {
		if key == "name" {
			fmt.Fprintf(&query, " AND name = %s", singleQuoted(value))
			continue
		}

		query.WriteString(" AND (")
		for i, prefix := range []string{
			"attrs",
			"scope_attrs",
			"resource_attrs",
		} {
			if i != 0 {
				query.WriteString(" OR ")
			}
			for i, column := range []string{
				"str",
				"int",
				"float",
				"bool",
				"bytes",
			} {
				if i != 0 {
					query.WriteString(" OR ")
				}
				fmt.Fprintf(&query,
					`toString( %[1]s_%[2]s_values[indexOf(%[1]s_%[2]s_keys, %[3]s)] ) = %[4]s`,
					prefix, column, singleQuoted(key), singleQuoted(value),
				)
			}
			query.WriteByte('\n')
		}
		query.WriteByte(')')
	}
	query.WriteByte(')')

	if s := opts.Start; s != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(start) >= %d", s)
	}
	if e := opts.End; e != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(end) <= %d", e)
	}
	if d := opts.MinDuration; d != 0 {
		fmt.Fprintf(&query, " AND (toUnixTimestamp64Nano(end) - toUnixTimestamp64Nano(start)) >= %d", d)
	}
	if d := opts.MaxDuration; d != 0 {
		fmt.Fprintf(&query, " AND (toUnixTimestamp64Nano(end) - toUnixTimestamp64Nano(start)) <= %d", d)
	}
	return q.querySpans(ctx, query.String())
}

// TagNames returns all available tag names.
func (q *Querier) TagNames(ctx context.Context) (r []string, rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "TagNames",
		trace.WithAttributes(
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	data := new(proto.ColStr).LowCardinality()
	if err := q.ch.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT DISTINCT name FROM %#q", table),
		Result: proto.ResultColumn{
			Name: "name",
			Data: data,
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			r = append(r, data.Values...)
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}
	return r, nil
}

// TagValues returns all available tag values for given tag.
func (q *Querier) TagValues(ctx context.Context, tagName string) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "TagValues",
		trace.WithAttributes(
			attribute.String("chstorage.tag_to_query", tagName),
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
		value     proto.ColStr
		valueType proto.ColEnum8

		r []tracestorage.Tag
	)

	if err := q.ch.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT DISTINCT value, value_type FROM %#q WHERE name = %s", table, singleQuoted(tagName)),
		Result: proto.Results{
			{Name: "value", Data: &value},
			{Name: "value_type", Data: proto.Wrap(&valueType, valueTypeDDL)},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			return value.ForEach(func(i int, value string) error {
				typ := valueType.Row(i)
				r = append(r, tracestorage.Tag{
					Name:  tagName,
					Value: value,
					Type:  int32(typ),
				})
				return nil
			})
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}

	return iterators.Slice(r), nil
}

// TraceByID returns spans of given trace.
func (q *Querier) TraceByID(ctx context.Context, id otelstorage.TraceID, opts tracestorage.TraceByIDOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "TraceByID",
		trace.WithAttributes(
			attribute.String("chstorage.id_to_query", id.Hex()),
			attribute.Int64("chstorage.start_range", int64(opts.Start)),
			attribute.Int64("chstorage.end_range", int64(opts.End)),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	query := fmt.Sprintf("SELECT * FROM %#q WHERE trace_id = %s", table, singleQuoted(id.Hex()))
	if s := opts.Start; s != 0 {
		query += fmt.Sprintf(" AND toUnixTimestamp64Nano(start) >= %d", s)
	}
	if e := opts.End; e != 0 {
		query += fmt.Sprintf(" AND toUnixTimestamp64Nano(end) <= %d", e)
	}
	return q.querySpans(ctx, query)
}

var _ traceqlengine.Querier = (*Querier)(nil)

// SelectSpansets get spansets from storage.
func (q *Querier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (_ iterators.Iterator[traceqlengine.Trace], rerr error) {
	ctx, span := q.tracer.Start(ctx, "SelectSpansets",
		trace.WithAttributes(
			attribute.String("chstorage.span_matcher_operation", params.Op.String()),
			attribute.Int("chstorage.span_matchers", len(params.Matchers)),
			attribute.Int64("chstorage.start_range", int64(params.Start)),
			attribute.Int64("chstorage.end_range", int64(params.End)),
			attribute.Int64("chstorage.max_duration", int64(params.MaxDuration)),
			attribute.Int64("chstorage.min_duration", int64(params.MinDuration)),
			attribute.Int("chstorage.limit", params.Limit),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	query := q.buildSpansetsQuery(span, params)

	iter, err := q.querySpans(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "query traces")
	}
	defer func() {
		_ = iter.Close()
	}()

	var (
		traces = map[otelstorage.TraceID][]tracestorage.Span{}
		val    tracestorage.Span
	)
	for iter.Next(&val) {
		traces[val.TraceID] = append(traces[val.TraceID], val)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	var (
		result     = make([]traceqlengine.Trace, 0, len(traces))
		spansCount int
	)
	for id, spans := range traces {
		spansCount += len(spans)
		result = append(result, traceqlengine.Trace{
			TraceID: id,
			Spans:   spans,
		})
	}
	span.SetAttributes(
		attribute.Int("chstorage.queried_spans", spansCount),
		attribute.Int("chstorage.queried_traces", len(result)),
	)

	return iterators.Slice(result), nil
}

func (q *Querier) buildSpansetsQuery(span trace.Span, params traceqlengine.SelectSpansetsParams) string {
	var (
		query strings.Builder
		table = q.tables.Spans
	)

	fmt.Fprintf(&query, `SELECT * FROM %#[1]q WHERE trace_id IN (
		SELECT trace_id FROM %#[1]q WHERE true
	`, table)

	var (
		dropped   int
		writeNext = func() {
			if params.Op == traceql.SpansetOpAnd {
				query.WriteString("\nAND ")
			} else {
				query.WriteString("\nOR ")
			}
		}
	)
	for _, matcher := range params.Matchers {
		if matcher.Op == 0 {
			writeNext()

			// Just query spans with this attribute.
			attr := matcher.Attribute
			query.WriteString("(\n")
			for i, prefix := range getAttributeColumns(attr) {
				if i != 0 {
					query.WriteString(" OR ")
				}
				for i, column := range []string{
					"str",
					"int",
					"float",
					"bool",
					"bytes",
				} {
					if i != 0 {
						query.WriteString(" OR ")
					}
					fmt.Fprintf(&query,
						`has(%s_%s_keys, %s)`,
						prefix, column, singleQuoted(attr.Name),
					)
				}
				query.WriteByte('\n')
			}
			query.WriteString("\n)")
		}

		var cmp string
		switch matcher.Op {
		case traceql.OpEq:
			cmp = "="
		case traceql.OpNotEq:
			cmp = "!="
		case traceql.OpGt:
			cmp = ">"
		case traceql.OpGte:
			cmp = ">="
		case traceql.OpLt:
			cmp = "<"
		case traceql.OpLte:
			cmp = "<="
		case traceql.OpRe:
			cmp = "REGEXP"
		default:
			// Unsupported for now.
			dropped++
			continue
		}

		var value, typeSuffix string
		switch s := matcher.Static; s.Type {
		case traceql.TypeString:
			value = singleQuoted(s.Str)
			typeSuffix = "str"
		case traceql.TypeInt:
			value = strconv.FormatInt(s.AsInt(), 10)
			typeSuffix = "int"
		case traceql.TypeNumber:
			value = strconv.FormatFloat(s.AsNumber(), 'f', -1, 64)
			typeSuffix = "float"
		case traceql.TypeBool:
			if s.AsBool() {
				value = "true"
			} else {
				value = "false"
			}
			typeSuffix = "bool"
		case traceql.TypeDuration:
			value = strconv.FormatInt(s.AsDuration().Nanoseconds(), 10)
			typeSuffix = "int"
		case traceql.TypeSpanStatus:
			value = strconv.Itoa(int(s.AsSpanStatus()))
			typeSuffix = "int"
		case traceql.TypeSpanKind:
			value = strconv.Itoa(int(s.AsSpanKind()))
			typeSuffix = "int"
		default:
			// Unsupported for now.
			dropped++
			continue
		}

		writeNext()
		switch attr := matcher.Attribute; attr.Prop {
		case traceql.SpanDuration:
			fmt.Fprintf(&query, "(toUnixTimestamp64Nano(end)-toUnixTimestamp64Nano(start)) %s %s", cmp, value)
		case traceql.SpanName:
			fmt.Fprintf(&query, "name %s %s", cmp, value)
		case traceql.SpanStatus:
			fmt.Fprintf(&query, "status_code %s %s", cmp, value)
		case traceql.SpanKind:
			fmt.Fprintf(&query, "kind %s %s", cmp, value)
		case traceql.SpanParent,
			traceql.SpanChildCount,
			traceql.RootSpanName,
			traceql.RootServiceName,
			traceql.TraceDuration:
			// Unsupported yet.
			dropped++
			query.WriteString("true")
		default:
			// SpanAttribute
			query.WriteString("(\n")
			for i, column := range getAttributeColumns(attr) {
				if i != 0 {
					query.WriteString("\nOR ")
				}
				fmt.Fprintf(&query, "%[1]s_%[2]s_values[indexOf(%[1]s_%[2]s_keys, %[3]s)] %[4]s %[5]s",
					column, typeSuffix,
					singleQuoted(attr.Name),
					cmp, value,
				)
			}
			query.WriteString("\n)")
		}
	}
	query.WriteString("\n)")
	if s := params.Start; s != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(start) >= %d", s)
	}
	if e := params.End; e != 0 {
		fmt.Fprintf(&query, " AND toUnixTimestamp64Nano(end) <= %d", e)
	}

	span.SetAttributes(
		attribute.Int("chstorage.unsupported_span_matchers", dropped),
		attribute.String("chstorage.table", table),
	)
	return query.String()
}

func getAttributeColumns(attr traceql.Attribute) []string {
	if attr.Prop != traceql.SpanAttribute || attr.Parent {
		return nil
	}
	switch attr.Scope {
	case traceql.ScopeNone:
		return []string{
			"attrs",
			"scope_attrs",
			"resource_attrs",
		}
	case traceql.ScopeResource:
		return []string{
			"scope_attrs",
			"resource_attrs",
		}
	case traceql.ScopeSpan:
		return []string{
			"attrs",
		}
	default:
		return nil
	}
}

func (q *Querier) querySpans(ctx context.Context, query string) (iterators.Iterator[tracestorage.Span], error) {
	c := newSpanColumns()

	var r []tracestorage.Span
	if err := q.ch.Do(ctx, ch.Query{
		Body:   query,
		Result: c.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			r = c.ReadRowsTo(r)
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}

	return iterators.Slice(r), nil
}
