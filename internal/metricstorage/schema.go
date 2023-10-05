package metricstorage

import (
	"go.ytsaurus.tech/yt/go/schema"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Point is a data structure for metric points.
type Point struct {
	Metric        string                `json:"metric_name" yson:"metric_name"`
	ResourceHash  otelstorage.Hash      `json:"resource_hash" yson:"resource_hash"`
	AttributeHash otelstorage.Hash      `json:"attr_hash" yson:"attr_hash"`
	Timestamp     otelstorage.Timestamp `json:"timestamp" yson:"timestamp"`
	Point         float64               `json:"point" yson:"point"`
}

// YTSchema returns YTsaurus table schema for this structure.
func (Point) YTSchema() schema.Schema {
	var (
		tsType   = schema.TypeUint64
		hashType = schema.TypeBytes
	)

	return schema.Schema{
		Columns: []schema.Column{
			{Name: "metric_name", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "resource_hash", ComplexType: hashType, SortOrder: schema.SortAscending},
			{Name: "attr_hash", ComplexType: hashType, SortOrder: schema.SortAscending},
			{Name: "timestamp", ComplexType: tsType, SortOrder: schema.SortAscending},
			{Name: "point", ComplexType: schema.TypeFloat64},
		},
	}
}

// Resource is a data structure for resource.
type Resource struct {
	Hash  otelstorage.Hash  `json:"hash" yson:"hash"`
	Attrs otelstorage.Attrs `json:"attrs" yson:"attrs"`
}

// YTSchema returns YTsaurus table schema for this structure.
func (Resource) YTSchema() schema.Schema {
	var (
		hashType  = schema.TypeBytes
		attrsType = schema.Optional{Item: schema.TypeAny}
	)

	return schema.Schema{
		Columns: []schema.Column{
			{Name: "hash", ComplexType: hashType, SortOrder: schema.SortAscending},
			{Name: "attrs", ComplexType: attrsType},
		},
	}
}

// Attributes is a data structure for attributes.
type Attributes struct {
	Metric string            `json:"metric_name" yson:"metric_name"`
	Hash   otelstorage.Hash  `json:"hash" yson:"hash"`
	Attrs  otelstorage.Attrs `json:"attrs" yson:"attrs"`
}

// YTSchema returns YTsaurus table schema for this structure.
func (Attributes) YTSchema() schema.Schema {
	var (
		hashType  = schema.TypeBytes
		attrsType = schema.Optional{Item: schema.TypeAny}
	)

	return schema.Schema{
		Columns: []schema.Column{
			{Name: "metric_name", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "hash", ComplexType: hashType, SortOrder: schema.SortAscending},
			{Name: "attrs", ComplexType: attrsType},
		},
	}
}
