package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type logColumns struct {
	timestamp         *proto.ColDateTime64
	observedTimestamp *proto.ColDateTime64
	flags             proto.ColUInt32
	severityNumber    proto.ColInt32
	severityText      *proto.ColLowCardinality[string]
	body              proto.ColStr
	traceID           proto.ColRawOf[otelstorage.TraceID]
	spanID            proto.ColRawOf[otelstorage.SpanID]
	attributes        proto.ColStr
	resource          proto.ColStr
	scopeName         proto.ColStr
	scopeVersion      proto.ColStr
	scopeAttributes   proto.ColStr
}

func newLogColumns() *logColumns {
	return &logColumns{
		timestamp:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		observedTimestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		severityText:      new(proto.ColStr).LowCardinality(),
	}
}

func (c *logColumns) StaticColumns() []string {
	var cols []string
	for _, col := range c.Input() {
		cols = append(cols, col.Name)
	}
	return cols
}

func (c *logColumns) AddRow(r logstorage.Record) {
	c.timestamp.Append(r.Timestamp.AsTime())
	c.observedTimestamp.Append(r.ObservedTimestamp.AsTime())
	c.flags.Append(uint32(r.Flags))
	c.severityNumber.Append(int32(r.SeverityNumber))
	c.severityText.Append(r.SeverityText)
	c.body.Append(r.Body)
	c.traceID.Append(r.TraceID)
	c.spanID.Append(r.SpanID)
	c.attributes.Append(encodeAttributes(r.Attrs.AsMap()))
	c.resource.Append(encodeAttributes(r.ResourceAttrs.AsMap()))
	c.scopeName.Append(r.ScopeName)
	c.scopeVersion.Append(r.ScopeVersion)
	c.scopeAttributes.Append(encodeAttributes(r.ScopeAttrs.AsMap()))
}

func (c *logColumns) Input() proto.Input {
	return proto.Input{
		{Name: "timestamp", Data: c.timestamp},
		{Name: "observed_timestamp", Data: c.observedTimestamp},
		{Name: "flags", Data: c.flags},
		{Name: "severity_number", Data: c.severityNumber},
		{Name: "severity_text", Data: c.severityText},
		{Name: "body", Data: c.body},
		{Name: "trace_id", Data: c.traceID},
		{Name: "span_id", Data: c.spanID},
		{Name: "attributes", Data: c.attributes},
		{Name: "resource", Data: c.resource},
		{Name: "scope_name", Data: c.scopeName},
		{Name: "scope_version", Data: c.scopeVersion},
		{Name: "scope_attributes", Data: c.scopeAttributes},
	}
}

func (c *logColumns) Result() proto.Results {
	return proto.Results{
		{Name: "timestamp", Data: c.timestamp},
		{Name: "observed_timestamp", Data: c.observedTimestamp},
		{Name: "flags", Data: &c.flags},
		{Name: "severity_number", Data: &c.severityNumber},
		{Name: "severity_text", Data: c.severityText},
		{Name: "body", Data: &c.body},
		{Name: "trace_id", Data: &c.traceID},
		{Name: "span_id", Data: &c.spanID},
		{Name: "attributes", Data: &c.attributes},
		{Name: "resource", Data: &c.resource},
		{Name: "scope_name", Data: &c.scopeName},
		{Name: "scope_version", Data: &c.scopeVersion},
		{Name: "scope_attributes", Data: &c.scopeAttributes},
	}
}
