package chstorage

import "github.com/ClickHouse/ch-go/proto"

type metricColumns struct {
	name       *proto.ColLowCardinality[string]
	ts         *proto.ColDateTime64
	value      proto.ColFloat64
	attributes proto.ColStr
	resource   proto.ColStr
}

func newMetricColumns() *metricColumns {
	return &metricColumns{
		name: new(proto.ColStr).LowCardinality(),
		ts:   new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
	}
}

func (c *metricColumns) StaticColumns() []string {
	return []string{
		"name",
		"ts",
		"value",
		"attributes",
		"resource",
	}
}

func (c *metricColumns) Input() proto.Input {
	input := proto.Input{
		{Name: "name", Data: c.name},
		{Name: "ts", Data: c.ts},
		{Name: "value", Data: c.value},
		{Name: "attributes", Data: c.attributes},
		{Name: "resource", Data: c.resource},
	}
	return input
}

func (c *metricColumns) Result() proto.Results {
	return proto.Results{
		{Name: "name", Data: c.name},
		{Name: "ts", Data: c.ts},
		{Name: "value", Data: &c.value},
		{Name: "attributes", Data: &c.attributes},
		{Name: "resource", Data: &c.resource},
	}
}

type labelsColumns struct {
	name  *proto.ColLowCardinality[string]
	value proto.ColStr
}

func newLabelsColumns() *labelsColumns {
	return &labelsColumns{
		name: new(proto.ColStr).LowCardinality(),
	}
}

func (c *labelsColumns) Input() proto.Input {
	input := proto.Input{
		{Name: "name", Data: c.name},
		{Name: "value", Data: c.value},
	}
	return input
}

func (c *labelsColumns) Result() proto.Results {
	return proto.Results{
		{Name: "name", Data: c.name},
		{Name: "value", Data: &c.value},
	}
}
