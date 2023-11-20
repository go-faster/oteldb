package chstorage

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/google/uuid"

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
	statusCode    proto.ColInt32
	statusMessage proto.ColStr

	batchID    proto.ColUUID
	attributes proto.ColStr
	resource   proto.ColStr

	scopeName       proto.ColStr
	scopeVersion    proto.ColStr
	scopeAttributes proto.ColStr

	events eventsColumns
	links  linksColumns
}

func newSpanColumns() *spanColumns {
	return &spanColumns{
		name:   new(proto.ColStr).LowCardinality(),
		start:  new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		end:    new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		events: newEventsColumns(),
		links:  newLinksColumns(),
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
		{Name: "status_code", Data: c.statusCode},
		{Name: "status_message", Data: c.statusMessage},

		{Name: "batch_id", Data: c.batchID},
		{Name: "attributes", Data: c.attributes},
		{Name: "resource", Data: c.resource},

		{Name: "scope_name", Data: c.scopeName},
		{Name: "scope_version", Data: c.scopeVersion},
		{Name: "scope_attributes", Data: c.scopeAttributes},

		{Name: "events_timestamps", Data: c.events.timestamps},
		{Name: "events_names", Data: c.events.names},
		{Name: "events_attributes", Data: c.events.attributes},

		{Name: "links_trace_ids", Data: c.links.traceIDs},
		{Name: "links_span_ids", Data: c.links.spanIDs},
		{Name: "links_tracestates", Data: c.links.tracestates},
		{Name: "links_attributes", Data: c.links.attributes},
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
		{Name: "status_code", Data: &c.statusCode},
		{Name: "status_message", Data: &c.statusMessage},

		{Name: "batch_id", Data: &c.batchID},
		{Name: "attributes", Data: &c.attributes},
		{Name: "resource", Data: &c.resource},

		{Name: "scope_name", Data: &c.scopeName},
		{Name: "scope_version", Data: &c.scopeVersion},
		{Name: "scope_attributes", Data: &c.scopeAttributes},

		{Name: "events_timestamps", Data: c.events.timestamps},
		{Name: "events_names", Data: c.events.names},
		{Name: "events_attributes", Data: c.events.attributes},

		{Name: "links_trace_ids", Data: c.links.traceIDs},
		{Name: "links_span_ids", Data: c.links.spanIDs},
		{Name: "links_tracestates", Data: c.links.tracestates},
		{Name: "links_attributes", Data: c.links.attributes},
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
	c.statusCode.Append(s.StatusCode)
	c.statusMessage.Append(s.StatusMessage)

	// FIXME(tdakkota): use UUID in Span.
	c.batchID.Append(uuid.MustParse(s.BatchID))
	c.attributes.Append(encodeAttributes(s.Attrs.AsMap()))
	c.resource.Append(encodeAttributes(s.ResourceAttrs.AsMap()))

	c.scopeName.Append(s.ScopeName)
	c.scopeVersion.Append(s.ScopeVersion)
	c.scopeAttributes.Append(encodeAttributes(s.ScopeAttrs.AsMap()))

	c.events.AddRow(s.Events)
	c.links.AddRow(s.Links)
}

func (c *spanColumns) ReadRowsTo(spans []tracestorage.Span) ([]tracestorage.Span, error) {
	for i := 0; i < c.traceID.Rows(); i++ {
		attrs, err := decodeAttributes(c.attributes.Row(i))
		if err != nil {
			return nil, errors.Wrap(err, "decode attributes")
		}
		resource, err := decodeAttributes(c.resource.Row(i))
		if err != nil {
			return nil, errors.Wrap(err, "decode resource")
		}
		scopeAttrs, err := decodeAttributes(c.scopeAttributes.Row(i))
		if err != nil {
			return nil, errors.Wrap(err, "decode scope attributes")
		}
		events, err := c.events.Row(i)
		if err != nil {
			return nil, errors.Wrap(err, "decode events")
		}
		links, err := c.links.Row(i)
		if err != nil {
			return nil, errors.Wrap(err, "decode links")
		}

		spans = append(spans, tracestorage.Span{
			TraceID:       otelstorage.TraceID(c.traceID.Row(i)),
			SpanID:        otelstorage.SpanIDFromUint64(c.spanID.Row(i)),
			TraceState:    c.traceState.Row(i),
			ParentSpanID:  otelstorage.SpanIDFromUint64(c.parentSpanID.Row(i)),
			Name:          c.name.Row(i),
			Kind:          int32(c.kind.Row(i)),
			Start:         otelstorage.NewTimestampFromTime(c.start.Row(i)),
			End:           otelstorage.NewTimestampFromTime(c.end.Row(i)),
			Attrs:         attrs,
			StatusCode:    c.statusCode.Row(i),
			StatusMessage: c.statusMessage.Row(i),
			BatchID:       c.batchID.Row(i).String(),
			ResourceAttrs: resource,
			ScopeName:     c.scopeName.Row(i),
			ScopeVersion:  c.scopeVersion.Row(i),
			ScopeAttrs:    scopeAttrs,
			Events:        events,
			Links:         links,
		})
	}

	return spans, nil
}

type eventsColumns struct {
	names      *proto.ColArr[string]
	timestamps *proto.ColArr[time.Time]
	attributes *proto.ColArr[string]
}

func newEventsColumns() eventsColumns {
	return eventsColumns{
		names:      new(proto.ColStr).Array(),
		timestamps: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).Array(),
		attributes: new(proto.ColStr).Array(),
	}
}

func (c *eventsColumns) AddRow(events []tracestorage.Event) {
	var (
		names      []string
		timestamps []time.Time
		attrs      []string
	)
	for _, e := range events {
		names = append(names, e.Name)
		timestamps = append(timestamps, time.Unix(0, int64(e.Timestamp)))
		attrs = append(attrs, encodeAttributes(e.Attrs.AsMap()))
	}

	c.names.Append(names)
	c.timestamps.Append(timestamps)
	c.attributes.Append(attrs)
}

func (c *eventsColumns) Row(row int) (events []tracestorage.Event, _ error) {
	var (
		names      = c.names.Row(row)
		timestamps = c.timestamps.Row(row)
		attributes = c.attributes.Row(row)

		l = min(
			len(names),
			len(timestamps),
			len(attributes),
		)
	)
	for i := 0; i < l; i++ {
		attrs, err := decodeAttributes(attributes[i])
		if err != nil {
			return nil, errors.Wrap(err, "decode attributes")
		}

		events = append(events, tracestorage.Event{
			Name:      names[i],
			Timestamp: otelstorage.NewTimestampFromTime(timestamps[i]),
			Attrs:     attrs,
		})
	}
	return events, nil
}

type linksColumns struct {
	traceIDs    *proto.ColArr[uuid.UUID]
	spanIDs     *proto.ColArr[uint64]
	tracestates *proto.ColArr[string]
	attributes  *proto.ColArr[string]
}

func newLinksColumns() linksColumns {
	return linksColumns{
		traceIDs:    new(proto.ColUUID).Array(),
		spanIDs:     new(proto.ColUInt64).Array(),
		tracestates: new(proto.ColStr).Array(),
		attributes:  new(proto.ColStr).Array(),
	}
}

func (c *linksColumns) AddRow(links []tracestorage.Link) {
	var (
		traceIDs    []uuid.UUID
		spanIDs     []uint64
		tracestates []string
		attributes  []string
	)
	for _, l := range links {
		traceIDs = append(traceIDs, uuid.UUID(l.TraceID))
		spanIDs = append(spanIDs, l.SpanID.AsUint64())
		tracestates = append(tracestates, l.TraceState)
		attributes = append(attributes, encodeAttributes(l.Attrs.AsMap()))
	}

	c.traceIDs.Append(traceIDs)
	c.spanIDs.Append(spanIDs)
	c.tracestates.Append(tracestates)
	c.attributes.Append(attributes)
}

func (c *linksColumns) Row(row int) (links []tracestorage.Link, _ error) {
	var (
		traceIDs    = c.traceIDs.Row(row)
		spanIDs     = c.spanIDs.Row(row)
		tracestates = c.tracestates.Row(row)
		attributes  = c.attributes.Row(row)

		l = min(
			len(traceIDs),
			len(spanIDs),
			len(tracestates),
			len(attributes),
		)
	)
	for i := 0; i < l; i++ {
		attrs, err := decodeAttributes(attributes[i])
		if err != nil {
			return nil, errors.Wrap(err, "decode attributes")
		}

		links = append(links, tracestorage.Link{
			TraceID:    otelstorage.TraceID(traceIDs[i]),
			SpanID:     otelstorage.SpanIDFromUint64(spanIDs[i]),
			TraceState: tracestates[i],
			Attrs:      attrs,
		})
	}
	return links, nil
}
