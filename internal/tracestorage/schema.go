package tracestorage

import (
	"go.ytsaurus.tech/yt/go/schema"
)

// Timestamp is a tracestorage timestamp.
type Timestamp = uint64

// Span is a data structure for span.
type Span struct {
	TraceID       TraceID   `json:"trace_id" yson:"trace_id"`
	SpanID        SpanID    `json:"span_id" yson:"span_id"`
	TraceState    string    `json:"trace_state" yson:"trace_state"`
	ParentSpanID  SpanID    `json:"parent_span_id" yson:"parent_span_id"`
	Name          string    `json:"name" yson:"name"`
	Kind          int32     `json:"kind" yson:"kind"`
	Start         Timestamp `json:"start" yson:"start"`
	End           Timestamp `json:"end" yson:"end"`
	Attrs         Attrs     `json:"attrs" yson:"attrs"`
	StatusCode    int32     `json:"status_code" yson:"status_code"`
	StatusMessage string    `json:"status_message" yson:"status_message"`

	BatchID       string `json:"batch_id" yson:"batch_id"`
	ResourceAttrs Attrs  `json:"resource_attrs" yson:"resource_attrs"`

	ScopeName    string `json:"scope_name" yson:"scope_name"`
	ScopeVersion string `json:"scope_version" yson:"scope_version"`
	ScopeAttrs   Attrs  `json:"scope_attrs" yson:"scope_attrs"`
}

// YTSchema returns YTsaurus table schema for this structure.
func (Span) YTSchema() schema.Schema {
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			// FIXME(tdakkota): where is UUID?
			{Name: "trace_id", ComplexType: schema.TypeBytes, SortOrder: schema.SortAscending},
			{Name: "span_id", ComplexType: schema.TypeUint64, SortOrder: schema.SortAscending},
			{Name: "trace_state", ComplexType: schema.TypeString},
			{Name: "parent_span_id", ComplexType: schema.Optional{Item: schema.TypeUint64}},
			{Name: "name", ComplexType: schema.TypeString},
			{Name: "kind", ComplexType: schema.TypeInt32},
			// Start and end are nanoseconds, so we can't use Timestamp.
			{Name: "start", ComplexType: schema.TypeUint64},
			{Name: "end", ComplexType: schema.TypeUint64},
			{Name: "attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},
			{Name: "status_code", ComplexType: schema.TypeInt32},
			{Name: "status_message", ComplexType: schema.TypeString},

			{Name: "batch_id", ComplexType: schema.TypeString},
			{Name: "resource_attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},

			{Name: "scope_name", ComplexType: schema.TypeString},
			{Name: "scope_version", ComplexType: schema.TypeString},
			{Name: "scope_attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},
		},
	}
}

// Tag is a data structure for tag.
type Tag struct {
	Name  string `yson:"name"`
	Value string `yson:"value"`
	Type  int32  `yson:"type"`
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
