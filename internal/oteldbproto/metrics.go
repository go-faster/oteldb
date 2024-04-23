package oteldbproto

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Metrics describes a metric batch.
type Metrics struct {
	Number NumberPoints

	Resources []Resource
	Scopes    []Scope
	Arrays    [][]Value
	Objects   [][]KeyValue
}

func (m *Metrics) pushResource() int {
	return appendZero(&m.Resources)
}

func (m *Metrics) pushScope() int {
	return appendZero(&m.Scopes)
}

// MetricHeader contains common fields for all types of metrics.
type MetricHeader struct {
	Resource []int
	Scope    []int
	Name     []string
	Unit     []string
}

// Push adds appends an empty element to the end of each column.
func (p *MetricHeader) Push() (idx int) {
	idx = appendZero(&p.Resource)
	appendZero(&p.Scope)
	appendZero(&p.Name)
	appendZero(&p.Unit)
	return idx
}

// NumberPoints describes a columnar batch of number metrics.
type NumberPoints struct {
	MetricHeader

	Attributes        []Attributes
	StartTimeUnixNano []uint64
	TimeUnixNano      []uint64
	Value             []Value // only Int and Double.
	Exemplar          [][]Exemplar
	Flags             []pmetric.DataPointFlags
}

// Push adds appends an empty element to the end of each column.
func (p *NumberPoints) Push() (idx int) {
	p.MetricHeader.Push()
	idx = appendZero(&p.Attributes)
	appendZero(&p.StartTimeUnixNano)
	appendZero(&p.TimeUnixNano)
	appendZero(&p.Value)
	appendZero(&p.Exemplar)
	appendZero(&p.Flags)
	return idx
}

// Exemplar is an OpenTelemetry exemplar.
type Exemplar struct {
	FilteredAttributes Attributes
	TimeUnixNano       uint64
	Value              Value // only Int and Double.
	SpanID             pcommon.SpanID
	TraceID            pcommon.TraceID
}
