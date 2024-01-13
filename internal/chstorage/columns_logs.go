package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

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
	attributes *Attributes
	resource   *Attributes

	scopeName       *proto.ColLowCardinality[string]
	scopeVersion    *proto.ColLowCardinality[string]
	scopeAttributes *Attributes
}

func newLogColumns() *logColumns {
	return &logColumns{
		serviceName:       new(proto.ColStr).LowCardinality(),
		serviceInstanceID: new(proto.ColStr).LowCardinality(),
		serviceNamespace:  new(proto.ColStr).LowCardinality(),
		timestamp:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		severityText:      new(proto.ColStr).LowCardinality(),
		attributes:        NewAttributes(colAttrs),
		resource:          NewAttributes(colResource),
		scopeName:         new(proto.ColStr).LowCardinality(),
		scopeVersion:      new(proto.ColStr).LowCardinality(),
		scopeAttributes:   NewAttributes(colScope),
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

func (c *logColumns) ForEach(f func(r logstorage.Record)) error {
	for i := 0; i < c.timestamp.Rows(); i++ {
		r := logstorage.Record{
			Timestamp:      otelstorage.NewTimestampFromTime(c.timestamp.Row(i)),
			SeverityText:   c.severityText.Row(i),
			SeverityNumber: plog.SeverityNumber(c.severityNumber.Row(i)),
			TraceID:        c.traceID.Row(i),
			SpanID:         c.spanID.Row(i),
			Flags:          plog.LogRecordFlags(c.traceFlags.Row(i)),
			Body:           c.body.Row(i),

			ScopeVersion: c.scopeVersion.Row(i),
			ScopeName:    c.scopeName.Row(i),
		}
		{
			a := c.resource.Row(i)
			v := a.AsMap()
			if s := c.serviceInstanceID.Row(i); s != "" {
				v.PutStr(string(semconv.ServiceInstanceIDKey), s)
			}
			if s := c.serviceName.Row(i); s != "" {
				v.PutStr(string(semconv.ServiceNameKey), s)
			}
			if s := c.serviceNamespace.Row(i); s != "" {
				v.PutStr(string(semconv.ServiceNamespaceKey), s)
			}
			r.ResourceAttrs = a
		}
		{
			r.Attrs = c.attributes.Row(i)
		}
		{
			r.ScopeAttrs = c.scopeAttributes.Row(i)
		}
		{
			// Default just to timestamp.
			r.ObservedTimestamp = r.Timestamp
		}
		f(r)
	}
	return nil
}

func (c *logColumns) AddRow(r logstorage.Record) {
	{
		m := r.ResourceAttrs.AsMap()
		setStrOrEmpty(c.serviceInstanceID, m, string(semconv.ServiceInstanceIDKey))
		setStrOrEmpty(c.serviceName, m, string(semconv.ServiceNameKey))
		setStrOrEmpty(c.serviceNamespace, m, string(semconv.ServiceNamespaceKey))
	}
	c.timestamp.Append(r.Timestamp.AsTime())

	c.severityNumber.Append(uint8(r.SeverityNumber))
	c.severityText.Append(r.SeverityText)

	c.traceID.Append(r.TraceID)
	c.spanID.Append(r.SpanID)
	c.traceFlags.Append(uint8(r.Flags))

	c.body.Append(r.Body)
	c.attributes.Append(r.Attrs)
	c.resource.Append(r.ResourceAttrs)

	c.scopeName.Append(r.ScopeName)
	c.scopeVersion.Append(r.ScopeVersion)
	c.scopeAttributes.Append(r.ScopeAttrs)
}

func (c *logColumns) columns() Columns {
	return MergeColumns(Columns{
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

		{Name: "scope_name", Data: c.scopeName},
		{Name: "scope_version", Data: c.scopeVersion},
	},
		c.attributes.Columns(),
		c.scopeAttributes.Columns(),
		c.resource.Columns(),
	)
}

func (c *logColumns) Input() proto.Input    { return c.columns().Input() }
func (c *logColumns) Result() proto.Results { return c.columns().Result() }
func (c *logColumns) Reset()                { c.columns().Reset() }

type logAttrMapColumns struct {
	name proto.ColStr // http_method
	key  proto.ColStr // http.method
}

func newLogAttrMapColumns() *logAttrMapColumns {
	return &logAttrMapColumns{}
}

func (c *logAttrMapColumns) columns() Columns {
	return []Column{
		{Name: "name", Data: &c.name},
		{Name: "key", Data: &c.key},
	}
}

func (c *logAttrMapColumns) Input() proto.Input    { return c.columns().Input() }
func (c *logAttrMapColumns) Result() proto.Results { return c.columns().Result() }
func (c *logAttrMapColumns) Reset()                { c.columns().Reset() }

func (c *logAttrMapColumns) ForEach(f func(name, key string)) {
	for i := 0; i < c.name.Rows(); i++ {
		f(c.name.Row(i), c.key.Row(i))
	}
}

func (c *logAttrMapColumns) AddAttrs(attrs otelstorage.Attrs) {
	attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
		c.AddRow(otelstorage.KeyToLabel(k), k)
		return true
	})
}

func (c *logAttrMapColumns) AddRow(name, key string) {
	c.name.Append(name)
	c.key.Append(key)
}
