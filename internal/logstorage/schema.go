package logstorage

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.ytsaurus.tech/yt/go/schema"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Record is a log record.
type Record struct {
	ObservedTimestamp otelstorage.Timestamp `json:"observed_timestamp" yson:"observed_timestamp"`
	Timestamp         otelstorage.Timestamp `json:"timestamp" yson:"timestamp"`
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
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{Name: `observed_timestamp`, ComplexType: schema.TypeUint64, SortOrder: schema.SortAscending},
			{Name: `timestamp`, ComplexType: schema.TypeUint64},
			{Name: `trace_id`, ComplexType: schema.TypeBytes},
			{Name: `span_id`, ComplexType: schema.TypeUint64},
			{Name: `flags`, ComplexType: schema.TypeUint32},
			{Name: `severity_text`, ComplexType: schema.TypeString},
			{Name: `severity_number`, ComplexType: schema.TypeInt32},
			{Name: `body`, ComplexType: schema.TypeString},
			{Name: `attrs`, ComplexType: schema.Optional{Item: schema.TypeAny}},
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
