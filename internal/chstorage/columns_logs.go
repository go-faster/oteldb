package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
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

	body            proto.ColStr
	attributes      *proto.ColMap[string, string]
	attributesTypes *proto.ColMap[string, uint8]
	resource        *proto.ColMap[string, string]
	resourceTypes   *proto.ColMap[string, uint8]

	scopeName            *proto.ColLowCardinality[string]
	scopeVersion         *proto.ColLowCardinality[string]
	scopeAttributes      *proto.ColMap[string, string]
	scopeAttributesTypes *proto.ColMap[string, uint8]
}

func newAttributesColumn() *proto.ColMap[string, string] {
	return proto.NewMap[string, string](
		new(proto.ColStr).LowCardinality(),
		new(proto.ColStr),
	)
}

func newTypesColumn() *proto.ColMap[string, uint8] {
	return proto.NewMap[string, uint8](
		new(proto.ColStr).LowCardinality(),
		new(proto.ColUInt8),
	)
}

func decodeAttributesRow(values *proto.ColMap[string, string], types *proto.ColMap[string, uint8], i int) (otelstorage.Attrs, error) {
	m := pcommon.NewMap()
	s := values.RowKV(i)
	st := types.RowKV(i)
	if len(s) != len(st) {
		return otelstorage.Attrs{}, errors.New("length mismatch")
	}
	for i, kv := range s {
		t := st[i].Value
		v := m.PutEmpty(kv.Key)
		if err := valueFromStrType(v, kv.Value, t); err != nil {
			return otelstorage.Attrs{}, errors.Wrap(err, "decode value")
		}
	}
	return otelstorage.Attrs(m), nil
}

type attrKV = proto.KV[string, string]
type typKV = proto.KV[string, uint8]

func valueToStrType(v pcommon.Value) (string, uint8) {
	if t := v.Type(); t == pcommon.ValueTypeStr {
		return v.Str(), uint8(t)
	}
	e := &jx.Encoder{}
	encodeValue(e, v)
	return e.String(), uint8(v.Type())
}

func valueFromStrType(v pcommon.Value, s string, t uint8) error {
	if t := pcommon.ValueType(t); t == pcommon.ValueTypeStr {
		v.SetStr(s)
		return nil
	}
	d := jx.DecodeStr(s)
	return decodeValue(d, v)
}

func encodeAttributesRow(attrs pcommon.Map) ([]attrKV, []typKV) {
	out := make([]attrKV, 0, attrs.Len())
	outTypes := make([]typKV, 0, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		s, t := valueToStrType(v)
		out = append(out, proto.KV[string, string]{
			Key:   k,
			Value: s,
		})
		outTypes = append(outTypes, proto.KV[string, uint8]{
			Key:   k,
			Value: t,
		})
		return true
	})
	return out, outTypes
}

func newLogColumns() *logColumns {
	return &logColumns{
		serviceName:          new(proto.ColStr).LowCardinality(),
		serviceInstanceID:    new(proto.ColStr).LowCardinality(),
		serviceNamespace:     new(proto.ColStr).LowCardinality(),
		timestamp:            new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		severityText:         new(proto.ColStr).LowCardinality(),
		attributes:           newAttributesColumn(),
		attributesTypes:      newTypesColumn(),
		resource:             newAttributesColumn(),
		resourceTypes:        newTypesColumn(),
		scopeName:            new(proto.ColStr).LowCardinality(),
		scopeVersion:         new(proto.ColStr).LowCardinality(),
		scopeAttributes:      newAttributesColumn(),
		scopeAttributesTypes: newTypesColumn(),
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
			m, err := decodeAttributesRow(c.resource, c.resourceTypes, i)
			if err != nil {
				return errors.Wrap(err, "decode resource")
			}
			v := m.AsMap()
			if s := c.serviceInstanceID.Row(i); s != "" {
				v.PutStr(string(semconv.ServiceInstanceIDKey), s)
			}
			if s := c.serviceName.Row(i); s != "" {
				v.PutStr(string(semconv.ServiceNameKey), s)
			}
			if s := c.serviceNamespace.Row(i); s != "" {
				v.PutStr(string(semconv.ServiceNamespaceKey), s)
			}
			r.ResourceAttrs = otelstorage.Attrs(v)
		}
		{
			m, err := decodeAttributesRow(c.attributes, c.attributesTypes, i)
			if err != nil {
				return errors.Wrap(err, "decode attributes")
			}
			r.Attrs = otelstorage.Attrs(m.AsMap())
		}
		{
			m, err := decodeAttributesRow(c.scopeAttributes, c.scopeAttributesTypes, i)
			if err != nil {
				return errors.Wrap(err, "decode scope attributes")
			}
			r.ScopeAttrs = otelstorage.Attrs(m.AsMap())
		}
		{
			// Default just to timestamp.
			r.ObservedTimestamp = r.Timestamp
		}
		f(r)
	}
	return nil
}

func appendAttributes(values *proto.ColMap[string, string], types *proto.ColMap[string, uint8], attrs otelstorage.Attrs) {
	a, t := encodeAttributesRow(attrs.AsMap())
	values.AppendKV(a)
	types.AppendKV(t)
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
	appendAttributes(c.attributes, c.attributesTypes, r.Attrs)
	appendAttributes(c.resource, c.resourceTypes, r.ResourceAttrs)

	c.scopeName.Append(r.ScopeName)
	c.scopeVersion.Append(r.ScopeVersion)
	appendAttributes(c.scopeAttributes, c.scopeAttributesTypes, r.ScopeAttrs)
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
		{Name: "attributes", Data: c.attributes},
		{Name: "attributes_types", Data: c.attributesTypes},
		{Name: "resource", Data: c.resource},
		{Name: "resource_types", Data: c.resourceTypes},

		{Name: "scope_name", Data: c.scopeName},
		{Name: "scope_version", Data: c.scopeVersion},
		{Name: "scope_attributes", Data: c.scopeAttributes},
		{Name: "scope_attributes_types", Data: c.scopeAttributesTypes},
	}
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

func (c *logAttrMapColumns) columns() tableColumns {
	return []tableColumn{
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
