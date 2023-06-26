package chstorage

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
	"golang.org/x/exp/constraints"

	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type spanColumns struct {
	traceID       proto.ColUUID
	spanID        proto.ColUInt64
	traceState    proto.ColStr
	parentSpanID  proto.ColUInt64
	name          *proto.ColLowCardinality[string]
	kind          proto.ColEnum8
	start         *proto.ColDateTime64
	end           *proto.ColDateTime64
	spanAttrs     chAttrs
	statusCode    proto.ColInt32
	statusMessage proto.ColStr

	batchID       proto.ColUUID
	resourceAttrs chAttrs

	scopeName    proto.ColStr
	scopeVersion proto.ColStr
	scopeAttrs   chAttrs

	events eventsColumns
	links  linksColumns
}

func newSpanColumns() *spanColumns {
	return &spanColumns{
		name:          new(proto.ColStr).LowCardinality(),
		start:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		end:           new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		spanAttrs:     newChAttrs(),
		resourceAttrs: newChAttrs(),
		scopeAttrs:    newChAttrs(),
		events:        newEventsColumns(),
		links:         newLinksColumns(),
	}
}

func (c *spanColumns) Input() proto.Input {
	return proto.Input{
		{Name: "trace_id", Data: c.traceID},
		{Name: "span_id", Data: c.spanID},
		{Name: "trace_state", Data: c.traceState},
		{Name: "parent_span_id", Data: c.parentSpanID},
		{Name: "name", Data: c.name},
		{Name: "kind", Data: proto.Wrap(&c.kind, kindDDL)},
		{Name: "start", Data: c.start},
		{Name: "end", Data: c.end},
		{Name: "attrs_str_keys", Data: c.spanAttrs.StrKeys},
		{Name: "attrs_str_values", Data: c.spanAttrs.StrValues},
		{Name: "attrs_int_keys", Data: c.spanAttrs.IntKeys},
		{Name: "attrs_int_values", Data: c.spanAttrs.IntValues},
		{Name: "attrs_float_keys", Data: c.spanAttrs.FloatKeys},
		{Name: "attrs_float_values", Data: c.spanAttrs.FloatValues},
		{Name: "attrs_bool_keys", Data: c.spanAttrs.BoolKeys},
		{Name: "attrs_bool_values", Data: c.spanAttrs.BoolValues},
		{Name: "attrs_bytes_keys", Data: c.spanAttrs.BytesKeys},
		{Name: "attrs_bytes_values", Data: c.spanAttrs.BytesValues},
		{Name: "status_code", Data: c.statusCode},
		{Name: "status_message", Data: c.statusMessage},

		{Name: "batch_id", Data: c.batchID},
		{Name: "resource_attrs_str_keys", Data: c.resourceAttrs.StrKeys},
		{Name: "resource_attrs_str_values", Data: c.resourceAttrs.StrValues},
		{Name: "resource_attrs_int_keys", Data: c.resourceAttrs.IntKeys},
		{Name: "resource_attrs_int_values", Data: c.resourceAttrs.IntValues},
		{Name: "resource_attrs_float_keys", Data: c.resourceAttrs.FloatKeys},
		{Name: "resource_attrs_float_values", Data: c.resourceAttrs.FloatValues},
		{Name: "resource_attrs_bool_keys", Data: c.resourceAttrs.BoolKeys},
		{Name: "resource_attrs_bool_values", Data: c.resourceAttrs.BoolValues},
		{Name: "resource_attrs_bytes_keys", Data: c.resourceAttrs.BytesKeys},
		{Name: "resource_attrs_bytes_values", Data: c.resourceAttrs.BytesValues},

		{Name: "scope_name", Data: c.scopeName},
		{Name: "scope_version", Data: c.scopeVersion},
		{Name: "scope_attrs_str_keys", Data: c.scopeAttrs.StrKeys},
		{Name: "scope_attrs_str_values", Data: c.scopeAttrs.StrValues},
		{Name: "scope_attrs_int_keys", Data: c.scopeAttrs.IntKeys},
		{Name: "scope_attrs_int_values", Data: c.scopeAttrs.IntValues},
		{Name: "scope_attrs_float_keys", Data: c.scopeAttrs.FloatKeys},
		{Name: "scope_attrs_float_values", Data: c.scopeAttrs.FloatValues},
		{Name: "scope_attrs_bool_keys", Data: c.scopeAttrs.BoolKeys},
		{Name: "scope_attrs_bool_values", Data: c.scopeAttrs.BoolValues},
		{Name: "scope_attrs_bytes_keys", Data: c.scopeAttrs.BytesKeys},
		{Name: "scope_attrs_bytes_values", Data: c.scopeAttrs.BytesValues},

		{Name: "events_timestamps", Data: c.events.Timestamps},
		{Name: "events_names", Data: c.events.Names},
		{Name: "events_attrs_str_keys", Data: c.events.Attrs.StrKeys},
		{Name: "events_attrs_str_values", Data: c.events.Attrs.StrValues},
		{Name: "events_attrs_int_keys", Data: c.events.Attrs.IntKeys},
		{Name: "events_attrs_int_values", Data: c.events.Attrs.IntValues},
		{Name: "events_attrs_float_keys", Data: c.events.Attrs.FloatKeys},
		{Name: "events_attrs_float_values", Data: c.events.Attrs.FloatValues},
		{Name: "events_attrs_bool_keys", Data: c.events.Attrs.BoolKeys},
		{Name: "events_attrs_bool_values", Data: c.events.Attrs.BoolValues},
		{Name: "events_attrs_bytes_keys", Data: c.events.Attrs.BytesKeys},
		{Name: "events_attrs_bytes_values", Data: c.events.Attrs.BytesValues},

		{Name: "links_trace_ids", Data: c.links.TraceIDs},
		{Name: "links_span_ids", Data: c.links.SpanIDs},
		{Name: "links_tracestates", Data: c.links.Tracestates},
		{Name: "links_attrs_str_keys", Data: c.links.Attrs.StrKeys},
		{Name: "links_attrs_str_values", Data: c.links.Attrs.StrValues},
		{Name: "links_attrs_int_keys", Data: c.links.Attrs.IntKeys},
		{Name: "links_attrs_int_values", Data: c.links.Attrs.IntValues},
		{Name: "links_attrs_float_keys", Data: c.links.Attrs.FloatKeys},
		{Name: "links_attrs_float_values", Data: c.links.Attrs.FloatValues},
		{Name: "links_attrs_bool_keys", Data: c.links.Attrs.BoolKeys},
		{Name: "links_attrs_bool_values", Data: c.links.Attrs.BoolValues},
		{Name: "links_attrs_bytes_keys", Data: c.links.Attrs.BytesKeys},
		{Name: "links_attrs_bytes_values", Data: c.links.Attrs.BytesValues},
	}
}

func (c *spanColumns) Result() proto.Results {
	return proto.Results{
		{Name: "trace_id", Data: &c.traceID},
		{Name: "span_id", Data: &c.spanID},
		{Name: "trace_state", Data: &c.traceState},
		{Name: "parent_span_id", Data: &c.parentSpanID},
		{Name: "name", Data: c.name},
		{Name: "kind", Data: &c.kind},
		{Name: "start", Data: c.start},
		{Name: "end", Data: c.end},
		{Name: "attrs_str_keys", Data: c.spanAttrs.StrKeys},
		{Name: "attrs_str_values", Data: c.spanAttrs.StrValues},
		{Name: "attrs_int_keys", Data: c.spanAttrs.IntKeys},
		{Name: "attrs_int_values", Data: c.spanAttrs.IntValues},
		{Name: "attrs_float_keys", Data: c.spanAttrs.FloatKeys},
		{Name: "attrs_float_values", Data: c.spanAttrs.FloatValues},
		{Name: "attrs_bool_keys", Data: c.spanAttrs.BoolKeys},
		{Name: "attrs_bool_values", Data: c.spanAttrs.BoolValues},
		{Name: "attrs_bytes_keys", Data: c.spanAttrs.BytesKeys},
		{Name: "attrs_bytes_values", Data: c.spanAttrs.BytesValues},
		{Name: "status_code", Data: &c.statusCode},
		{Name: "status_message", Data: &c.statusMessage},

		{Name: "batch_id", Data: &c.batchID},
		{Name: "resource_attrs_str_keys", Data: c.resourceAttrs.StrKeys},
		{Name: "resource_attrs_str_values", Data: c.resourceAttrs.StrValues},
		{Name: "resource_attrs_int_keys", Data: c.resourceAttrs.IntKeys},
		{Name: "resource_attrs_int_values", Data: c.resourceAttrs.IntValues},
		{Name: "resource_attrs_float_keys", Data: c.resourceAttrs.FloatKeys},
		{Name: "resource_attrs_float_values", Data: c.resourceAttrs.FloatValues},
		{Name: "resource_attrs_bool_keys", Data: c.resourceAttrs.BoolKeys},
		{Name: "resource_attrs_bool_values", Data: c.resourceAttrs.BoolValues},
		{Name: "resource_attrs_bytes_keys", Data: c.resourceAttrs.BytesKeys},
		{Name: "resource_attrs_bytes_values", Data: c.resourceAttrs.BytesValues},

		{Name: "scope_name", Data: &c.scopeName},
		{Name: "scope_version", Data: &c.scopeVersion},
		{Name: "scope_attrs_str_keys", Data: c.scopeAttrs.StrKeys},
		{Name: "scope_attrs_str_values", Data: c.scopeAttrs.StrValues},
		{Name: "scope_attrs_int_keys", Data: c.scopeAttrs.IntKeys},
		{Name: "scope_attrs_int_values", Data: c.scopeAttrs.IntValues},
		{Name: "scope_attrs_float_keys", Data: c.scopeAttrs.FloatKeys},
		{Name: "scope_attrs_float_values", Data: c.scopeAttrs.FloatValues},
		{Name: "scope_attrs_bool_keys", Data: c.scopeAttrs.BoolKeys},
		{Name: "scope_attrs_bool_values", Data: c.scopeAttrs.BoolValues},
		{Name: "scope_attrs_bytes_keys", Data: c.scopeAttrs.BytesKeys},
		{Name: "scope_attrs_bytes_values", Data: c.scopeAttrs.BytesValues},

		{Name: "events_timestamps", Data: c.events.Timestamps},
		{Name: "events_names", Data: c.events.Names},
		{Name: "events_attrs_str_keys", Data: c.events.Attrs.StrKeys},
		{Name: "events_attrs_str_values", Data: c.events.Attrs.StrValues},
		{Name: "events_attrs_int_keys", Data: c.events.Attrs.IntKeys},
		{Name: "events_attrs_int_values", Data: c.events.Attrs.IntValues},
		{Name: "events_attrs_float_keys", Data: c.events.Attrs.FloatKeys},
		{Name: "events_attrs_float_values", Data: c.events.Attrs.FloatValues},
		{Name: "events_attrs_bool_keys", Data: c.events.Attrs.BoolKeys},
		{Name: "events_attrs_bool_values", Data: c.events.Attrs.BoolValues},
		{Name: "events_attrs_bytes_keys", Data: c.events.Attrs.BytesKeys},
		{Name: "events_attrs_bytes_values", Data: c.events.Attrs.BytesValues},

		{Name: "links_trace_ids", Data: c.links.TraceIDs},
		{Name: "links_span_ids", Data: c.links.SpanIDs},
		{Name: "links_tracestates", Data: c.links.Tracestates},
		{Name: "links_attrs_str_keys", Data: c.links.Attrs.StrKeys},
		{Name: "links_attrs_str_values", Data: c.links.Attrs.StrValues},
		{Name: "links_attrs_int_keys", Data: c.links.Attrs.IntKeys},
		{Name: "links_attrs_int_values", Data: c.links.Attrs.IntValues},
		{Name: "links_attrs_float_keys", Data: c.links.Attrs.FloatKeys},
		{Name: "links_attrs_float_values", Data: c.links.Attrs.FloatValues},
		{Name: "links_attrs_bool_keys", Data: c.links.Attrs.BoolKeys},
		{Name: "links_attrs_bool_values", Data: c.links.Attrs.BoolValues},
		{Name: "links_attrs_bytes_keys", Data: c.links.Attrs.BytesKeys},
		{Name: "links_attrs_bytes_values", Data: c.links.Attrs.BytesValues},
	}
}

func (c *spanColumns) AddRow(s tracestorage.Span) {
	c.traceID.Append(uuid.UUID(s.TraceID))
	c.spanID.Append(s.SpanID.AsUint64())
	c.traceState.Append(s.TraceState)
	c.parentSpanID.Append(s.ParentSpanID.AsUint64())
	c.name.Append(s.Name)
	c.kind.Append(proto.Enum8(s.Kind))
	c.start.Append(time.Unix(0, int64(s.Start)))
	c.end.Append(time.Unix(0, int64(s.End)))
	c.spanAttrs.Append(s.Attrs)
	c.statusCode.Append(s.StatusCode)
	c.statusMessage.Append(s.StatusMessage)
	// FIXME(tdakkota): use UUID in Span.
	c.batchID.Append(uuid.MustParse(s.BatchID))
	c.resourceAttrs.Append(s.ResourceAttrs)
	c.scopeName.Append(s.ScopeName)
	c.scopeVersion.Append(s.ScopeVersion)
	c.scopeAttrs.Append(s.ScopeAttrs)
	c.events.AddRow(s.Events)
	c.links.AddRow(s.Links)
}

func (c *spanColumns) ReadRowsTo(spans []tracestorage.Span) []tracestorage.Span {
	for i := 0; i < c.traceID.Rows(); i++ {
		spans = append(spans, tracestorage.Span{
			TraceID:       otelstorage.TraceID(c.traceID.Row(i)),
			SpanID:        otelstorage.SpanIDFromUint64(c.spanID.Row(i)),
			TraceState:    c.traceState.Row(i),
			ParentSpanID:  otelstorage.SpanIDFromUint64(c.parentSpanID.Row(i)),
			Name:          c.name.Row(i),
			Kind:          int32(c.kind.Row(i)),
			Start:         otelstorage.NewTimestampFromTime(c.start.Row(i)),
			End:           otelstorage.NewTimestampFromTime(c.end.Row(i)),
			Attrs:         c.spanAttrs.Row(i),
			StatusCode:    c.statusCode.Row(i),
			StatusMessage: c.statusMessage.Row(i),
			BatchID:       c.batchID.Row(i).String(),
			ResourceAttrs: c.resourceAttrs.Row(i),
			ScopeName:     c.scopeName.Row(i),
			ScopeVersion:  c.scopeVersion.Row(i),
			ScopeAttrs:    c.scopeAttrs.Row(i),
			Events:        c.events.Row(i),
			Links:         c.links.Row(i),
		})
	}

	return spans
}

type eventsColumns struct {
	Names      *proto.ColArr[string]
	Timestamps *proto.ColArr[time.Time]
	Attrs      chArrAttrs
}

func newEventsColumns() eventsColumns {
	return eventsColumns{
		Names:      new(proto.ColStr).Array(),
		Timestamps: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).Array(),
		Attrs:      newChArrAttrs(),
	}
}

func (c *eventsColumns) AddRow(events []tracestorage.Event) {
	var (
		names      []string
		timestamps []time.Time
		attrs      chArrAttrCollector
	)
	for _, e := range events {
		names = append(names, e.Name)
		timestamps = append(timestamps, time.Unix(0, int64(e.Timestamp)))
		attrs.Append(e.Attrs)
	}

	c.Names.Append(names)
	c.Timestamps.Append(timestamps)
	attrs.AddRow(&c.Attrs)
}

func (c *eventsColumns) Row(row int) (events []tracestorage.Event) {
	var (
		names      = c.Names.Row(row)
		timestamps = c.Timestamps.Row(row)
		attrs      = c.Attrs.Row(row)

		l = minimum(
			len(names),
			len(timestamps),
			len(attrs),
		)
	)
	for i := 0; i < l; i++ {
		events = append(events, tracestorage.Event{
			Name:      names[i],
			Timestamp: otelstorage.NewTimestampFromTime(timestamps[i]),
			Attrs:     attrs[i],
		})
	}
	return events
}

type linksColumns struct {
	TraceIDs    *proto.ColArr[uuid.UUID]
	SpanIDs     *proto.ColArr[uint64]
	Tracestates *proto.ColArr[string]
	Attrs       chArrAttrs
}

func newLinksColumns() linksColumns {
	return linksColumns{
		TraceIDs:    new(proto.ColUUID).Array(),
		SpanIDs:     new(proto.ColUInt64).Array(),
		Tracestates: new(proto.ColStr).Array(),
		Attrs:       newChArrAttrs(),
	}
}

func (c *linksColumns) AddRow(links []tracestorage.Link) {
	var (
		traceIDs    []uuid.UUID
		spanIDs     []uint64
		tracestates []string
		attrs       chArrAttrCollector
	)
	for _, l := range links {
		traceIDs = append(traceIDs, uuid.UUID(l.TraceID))
		spanIDs = append(spanIDs, l.SpanID.AsUint64())
		tracestates = append(tracestates, l.TraceState)
		attrs.Append(l.Attrs)
	}

	c.TraceIDs.Append(traceIDs)
	c.SpanIDs.Append(spanIDs)
	c.Tracestates.Append(tracestates)
	attrs.AddRow(&c.Attrs)
}

func (c *linksColumns) Row(row int) (links []tracestorage.Link) {
	var (
		traceIDs    = c.TraceIDs.Row(row)
		spanIDs     = c.SpanIDs.Row(row)
		tracestates = c.Tracestates.Row(row)
		attrs       = c.Attrs.Row(row)

		l = minimum(
			len(traceIDs),
			len(spanIDs),
			len(tracestates),
			len(attrs),
		)
	)
	for i := 0; i < l; i++ {
		links = append(links, tracestorage.Link{
			TraceID:    otelstorage.TraceID(traceIDs[i]),
			SpanID:     otelstorage.SpanIDFromUint64(spanIDs[i]),
			TraceState: tracestates[i],
			Attrs:      attrs[i],
		})
	}
	return links
}

func minimum[T constraints.Integer](vals ...T) (min T) {
	if len(vals) < 1 {
		return min
	}
	min = vals[0]
	for _, val := range vals[1:] {
		if val < min {
			min = val
		}
	}
	return min
}
