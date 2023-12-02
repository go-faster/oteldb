package logstorage

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.ytsaurus.tech/yt/go/schema"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Labels to use where prometheus compatible labels are required, e.g. loki.
const (
	LabelTraceID  = "trace_id"
	LabelSpanID   = "span_id"
	LabelSeverity = "level"
	LabelBody     = "msg"

	LabelServiceName       = "service_name"        // resource.service.name
	LabelServiceNamespace  = "service_namespace"   // resource.service.namespace
	LabelServiceInstanceID = "service_instance_id" // resource.service.instance.id
)

// Record is a log record.
type Record struct {
	Timestamp         otelstorage.Timestamp `json:"timestamp" yson:"timestamp"`
	ObservedTimestamp otelstorage.Timestamp `json:"observed_timestamp" yson:"observed_timestamp"`
	TraceID           otelstorage.TraceID   `json:"trace_id" yson:"trace_id"`
	SpanID            otelstorage.SpanID    `json:"span_id" yson:"span_id"`
	Flags             plog.LogRecordFlags   `json:"flags" yson:"flags"`
	SeverityText      string                `json:"severity_text" yson:"severity_text"`
	SeverityNumber    plog.SeverityNumber   `json:"severity_number" yson:"severity_number"`
	Body              string                `json:"body" yson:"body"`
	Attrs             otelstorage.Attrs     `json:"attrs" yson:"attrs"`

	ResourceAttrs otelstorage.Attrs `json:"resource_attrs" yson:"resource_attrs"`

	ScopeName    string            `json:"scope_name" yson:"scope_name"`
	ScopeVersion string            `json:"scope_version" yson:"scope_version"`
	ScopeAttrs   otelstorage.Attrs `json:"scope_attrs" yson:"scope_attrs"`
}

// YTSchema returns YTsaurus table schema for this structure.
func (Record) YTSchema() schema.Schema {
	attrsType := schema.Optional{Item: schema.TypeAny}

	return schema.Schema{
		Columns: []schema.Column{
			{Name: `timestamp`, ComplexType: schema.TypeUint64, SortOrder: schema.SortAscending},
			{Name: `observed_timestamp`, ComplexType: schema.TypeUint64},
			{Name: `trace_id`, ComplexType: schema.Optional{Item: schema.TypeBytes}},
			{Name: `span_id`, ComplexType: schema.Optional{Item: schema.TypeUint64}},
			{Name: `flags`, ComplexType: schema.TypeUint32},
			{Name: `severity_text`, ComplexType: schema.TypeString},
			{Name: `severity_number`, ComplexType: schema.TypeInt32},
			{Name: `body`, ComplexType: schema.TypeString},
			{Name: `attrs`, ComplexType: attrsType},

			{Name: `resource_attrs`, ComplexType: attrsType},

			{Name: "scope_name", ComplexType: schema.TypeString},
			{Name: "scope_version", ComplexType: schema.TypeString},
			{Name: "scope_attrs", ComplexType: attrsType},
		},
	}
}

// Label is a data structure for log label.
type Label struct {
	Name  string `json:"name" yson:"name"`
	Value string `json:"value" yson:"value"`
	Type  int32  `json:"type" yson:"type"`
}

// YTSchema returns YTsaurus table schema for this structure.
func (Label) YTSchema() schema.Schema {
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{Name: "name", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "value", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "type", ComplexType: schema.TypeInt32},
		},
	}
}
