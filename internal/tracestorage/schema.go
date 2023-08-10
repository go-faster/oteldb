package tracestorage

import (
	"go.ytsaurus.tech/yt/go/schema"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Span is a data structure for span.
type Span struct {
	TraceID       otelstorage.TraceID   `json:"trace_id" yson:"trace_id"`
	SpanID        otelstorage.SpanID    `json:"span_id" yson:"span_id"`
	TraceState    string                `json:"trace_state" yson:"trace_state"`
	ParentSpanID  otelstorage.SpanID    `json:"parent_span_id" yson:"parent_span_id"`
	Name          string                `json:"name" yson:"name"`
	Kind          int32                 `json:"kind" yson:"kind"`
	Start         otelstorage.Timestamp `json:"start" yson:"start"`
	End           otelstorage.Timestamp `json:"end" yson:"end"`
	Attrs         otelstorage.Attrs     `json:"attrs" yson:"attrs"`
	StatusCode    int32                 `json:"status_code" yson:"status_code"`
	StatusMessage string                `json:"status_message" yson:"status_message"`

	BatchID       string            `json:"batch_id" yson:"batch_id"`
	ResourceAttrs otelstorage.Attrs `json:"resource_attrs" yson:"resource_attrs"`

	ScopeName    string            `json:"scope_name" yson:"scope_name"`
	ScopeVersion string            `json:"scope_version" yson:"scope_version"`
	ScopeAttrs   otelstorage.Attrs `json:"scope_attrs" yson:"scope_attrs"`

	Events []Event `json:"events" yson:"events"`
	Links  []Link  `json:"links" yson:"links"`
}

// ServiceName gets service name from attributes.
func (span Span) ServiceName() (string, bool) {
	res := span.ResourceAttrs
	if res.IsZero() {
		return "", false
	}
	attr, ok := res.AsMap().Get("service.name")
	if !ok {
		return "", false
	}
	return attr.AsString(), true
}

// YTSchema returns YTsaurus table schema for this structure.
func (Span) YTSchema() schema.Schema {
	var (
		traceIDType = schema.TypeBytes
		spanIDType  = schema.TypeUint64
		tsType      = schema.TypeUint64
		attrsType   = schema.Optional{Item: schema.TypeAny}

		eventType = schema.Struct{
			Members: []schema.StructMember{
				{Name: "timestamp", Type: tsType},
				{Name: "name", Type: schema.TypeString},
				{Name: "attrs", Type: attrsType},
			},
		}
		linkType = schema.Struct{
			Members: []schema.StructMember{
				{Name: "trace_id", Type: traceIDType},
				{Name: "span_id", Type: spanIDType},
				{Name: "trace_state", Type: schema.TypeString},
				{Name: "attrs", Type: attrsType},
			},
		}
	)

	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			// FIXME(tdakkota): where is UUID?
			{Name: "trace_id", ComplexType: traceIDType, SortOrder: schema.SortAscending},
			{Name: "span_id", ComplexType: spanIDType, SortOrder: schema.SortAscending},
			{Name: "trace_state", ComplexType: schema.TypeString},
			{Name: "parent_span_id", ComplexType: schema.Optional{Item: spanIDType}},
			{Name: "name", ComplexType: schema.TypeString},
			{Name: "kind", ComplexType: schema.TypeInt32},
			// Start and end are nanoseconds, so we can't use Timestamp.
			{Name: "start", ComplexType: tsType},
			{Name: "end", ComplexType: tsType},
			{Name: "attrs", ComplexType: attrsType},
			{Name: "status_code", ComplexType: schema.TypeInt32},
			{Name: "status_message", ComplexType: schema.TypeString},

			{Name: "batch_id", ComplexType: schema.TypeString},
			{Name: "resource_attrs", ComplexType: attrsType},

			{Name: "scope_name", ComplexType: schema.TypeString},
			{Name: "scope_version", ComplexType: schema.TypeString},
			{Name: "scope_attrs", ComplexType: attrsType},

			{Name: "events", ComplexType: schema.List{Item: eventType}},
			{Name: "links", ComplexType: schema.List{Item: linkType}},
		},
	}
}

// Event is a Span event.
type Event struct {
	Timestamp otelstorage.Timestamp `json:"timestamp" yson:"timestamp"`
	Name      string                `json:"name" yson:"name"`
	Attrs     otelstorage.Attrs     `json:"attrs" yson:"attrs"`
}

// Link is a Span link.
type Link struct {
	TraceID    otelstorage.TraceID `json:"trace_id" yson:"trace_id"`
	SpanID     otelstorage.SpanID  `json:"span_id" yson:"span_id"`
	TraceState string              `json:"trace_state" yson:"trace_state"`
	Attrs      otelstorage.Attrs   `json:"attrs" yson:"attrs"`
}

// Tag is a data structure for tag.
type Tag struct {
	Name  string `json:"name" yson:"name"`
	Value string `json:"value" yson:"value"`
	Type  int32  `json:"type" yson:"type"`
}

// YTSchema returns YTsaurus table schema for this structure.
func (Tag) YTSchema() schema.Schema {
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{Name: "name", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "value", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "type", ComplexType: schema.TypeInt32},
		},
	}
}
