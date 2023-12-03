package metricstorage

import (
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Point is a data structure for metric points.
type Point struct {
	Metric        otelstorage.Hash      `json:"metric_name"`
	ResourceHash  otelstorage.Hash      `json:"resource_hash"`
	AttributeHash otelstorage.Hash      `json:"attr_hash"`
	Timestamp     otelstorage.Timestamp `json:"timestamp"`
	Point         float64               `json:"point"`
}

// Resource is a data structure for resource.
type Resource struct {
	Hash  otelstorage.Hash  `json:"hash"`
	Attrs otelstorage.Attrs `json:"attrs"`
}

// Attributes is a data structure for attributes.
type Attributes struct {
	Metric otelstorage.Hash  `json:"metric_name"`
	Hash   otelstorage.Hash  `json:"hash"`
	Attrs  otelstorage.Attrs `json:"attrs"`
}
