package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type logColumns struct {
	serviceInstanceID *proto.ColLowCardinality[string]
	serviceName       *proto.ColLowCardinality[string]
	serviceNamespace  *proto.ColLowCardinality[string]

	timestamp *proto.ColDateTime64

	severityText   *proto.ColLowCardinality[string]
	severityNumber proto.ColUInt8

	traceFlags proto.ColUInt8
	traceID    proto.ColRawOf[otelstorage.TraceID]
	spanID     proto.ColRawOf[otelstorage.SpanID]

	body       proto.ColStr
	attributes proto.ColStr
	resource   proto.ColStr

	scopeName       *proto.ColLowCardinality[string]
	scopeVersion    *proto.ColLowCardinality[string]
	scopeAttributes proto.ColStr
}

func newLogColumns() *logColumns {
	return &logColumns{
		serviceName:       new(proto.ColStr).LowCardinality(),
		serviceInstanceID: new(proto.ColStr).LowCardinality(),
		serviceNamespace:  new(proto.ColStr).LowCardinality(),
		timestamp:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		severityText:      new(proto.ColStr).LowCardinality(),
		scopeName:         new(proto.ColStr).LowCardinality(),
		scopeVersion:      new(proto.ColStr).LowCardinality(),
	}
}

func (c *logColumns) StaticColumns() []string {
	var cols []string
	for _, col := range c.Input() {
		cols = append(cols, col.Name)
	}
	return cols
}

func setStrOrEmpty(col proto.ColumnOf[string], m pcommon.Map, k string) {
	v, ok := m.Get(k)
	if !ok {
		col.Append("")
		return
	}
	col.Append(v.AsString())
}

func (c *logColumns) AddRow(r logstorage.Record) {
	{
		m := r.ResourceAttrs.AsMap()
		setStrOrEmpty(c.serviceInstanceID, m, "service.instance.id")
		setStrOrEmpty(c.serviceName, m, "service.name")
		setStrOrEmpty(c.serviceNamespace, m, "service.namespace")
	}
	c.timestamp.Append(r.Timestamp.AsTime())

	c.severityNumber.Append(uint8(r.SeverityNumber))
	c.severityText.Append(r.SeverityText)

	c.traceID.Append(r.TraceID)
	c.spanID.Append(r.SpanID)
	c.traceFlags.Append(uint8(r.Flags))

	c.body.Append(r.Body)
	c.attributes.Append(encodeAttributes(r.Attrs.AsMap()))
	c.resource.Append(encodeAttributes(r.ResourceAttrs.AsMap()))

	c.scopeName.Append(r.ScopeName)
	c.scopeVersion.Append(r.ScopeVersion)
	c.scopeAttributes.Append(encodeAttributes(r.ScopeAttrs.AsMap()))
}

func (c *logColumns) columns() tableColumns {
	return []tableColumn{
		{Name: "service_instance_id", Data: c.serviceInstanceID},
		{Name: "service_name", Data: c.serviceName},
		{Name: "service_namespace", Data: c.serviceNamespace},

		{Name: "timestamp", Data: c.timestamp},

		{Name: "severity_number", Data: &c.severityNumber},
		{Name: "severity_text", Data: c.severityText},

		{Name: "trace_id", Data: &c.traceID},
		{Name: "span_id", Data: &c.spanID},
		{Name: "trace_flags", Data: &c.traceFlags},

		{Name: "body", Data: &c.body},
		{Name: "attributes", Data: &c.attributes},
		{Name: "resource", Data: &c.resource},

		{Name: "scope_name", Data: c.scopeName},
		{Name: "scope_version", Data: c.scopeVersion},
		{Name: "scope_attributes", Data: &c.scopeAttributes},
	}
}

func (c *logColumns) Input() proto.Input    { return c.columns().Input() }
func (c *logColumns) Result() proto.Results { return c.columns().Result() }
