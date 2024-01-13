package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"
)

type pointColumns struct {
	name           *proto.ColLowCardinality[string]
	nameNormalized *proto.ColLowCardinality[string]
	timestamp      *proto.ColDateTime64

	mapping proto.ColEnum8
	value   proto.ColFloat64

	flags      proto.ColUInt8
	attributes *Attributes
	resource   *Attributes
}

func newPointColumns() *pointColumns {
	return &pointColumns{
		name:           new(proto.ColStr).LowCardinality(),
		nameNormalized: new(proto.ColStr).LowCardinality(),
		timestamp:      new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		attributes: NewAttributes(colAttrs),
		resource:   NewAttributes(colResource),
	}
}

func (c *pointColumns) Columns() Columns {
	return MergeColumns(Columns{
		{Name: "name", Data: c.name},
		{Name: "name_normalized", Data: c.nameNormalized},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "mapping", Data: proto.Wrap(&c.mapping, metricMappingDDL)},
		{Name: "value", Data: &c.value},

		{Name: "flags", Data: &c.flags},
	}, c.attributes.Columns(), c.resource.Columns())
}

func (c *pointColumns) Input() proto.Input    { return c.Columns().Input() }
func (c *pointColumns) Result() proto.Results { return c.Columns().Result() }

type expHistogramColumns struct {
	name           *proto.ColLowCardinality[string]
	nameNormalized *proto.ColLowCardinality[string]
	timestamp      *proto.ColDateTime64

	count                proto.ColUInt64
	sum                  *proto.ColNullable[float64]
	min                  *proto.ColNullable[float64]
	max                  *proto.ColNullable[float64]
	scale                proto.ColInt32
	zerocount            proto.ColUInt64
	positiveOffset       proto.ColInt32
	positiveBucketCounts *proto.ColArr[uint64]
	negativeOffset       proto.ColInt32
	negativeBucketCounts *proto.ColArr[uint64]

	flags      proto.ColUInt32
	attributes *Attributes
	resource   *Attributes
}

func newExpHistogramColumns() *expHistogramColumns {
	return &expHistogramColumns{
		name:           new(proto.ColStr).LowCardinality(),
		nameNormalized: new(proto.ColStr).LowCardinality(),
		timestamp:      new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		sum:                  new(proto.ColFloat64).Nullable(),
		min:                  new(proto.ColFloat64).Nullable(),
		max:                  new(proto.ColFloat64).Nullable(),
		positiveBucketCounts: new(proto.ColUInt64).Array(),
		negativeBucketCounts: new(proto.ColUInt64).Array(),

		attributes: NewAttributes(colAttrs),
		resource:   NewAttributes(colResource),
	}
}

func (c *expHistogramColumns) Columns() Columns {
	return MergeColumns(Columns{
		{Name: "name", Data: c.name},
		{Name: "name_normalized", Data: c.nameNormalized},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "exp_histogram_count", Data: &c.count},
		{Name: "exp_histogram_sum", Data: c.sum},
		{Name: "exp_histogram_min", Data: c.min},
		{Name: "exp_histogram_max", Data: c.max},
		{Name: "exp_histogram_scale", Data: &c.scale},
		{Name: "exp_histogram_zerocount", Data: &c.zerocount},
		{Name: "exp_histogram_positive_offset", Data: &c.positiveOffset},
		{Name: "exp_histogram_positive_bucket_counts", Data: c.positiveBucketCounts},
		{Name: "exp_histogram_negative_offset", Data: &c.negativeOffset},
		{Name: "exp_histogram_negative_bucket_counts", Data: c.negativeBucketCounts},

		{Name: "flags", Data: &c.flags},
	}, c.attributes.Columns(), c.resource.Columns())
}

func (c *expHistogramColumns) Input() proto.Input    { return c.Columns().Input() }
func (c *expHistogramColumns) Result() proto.Results { return c.Columns().Result() }

type labelsColumns struct {
	name           *proto.ColLowCardinality[string]
	nameNormalized *proto.ColLowCardinality[string]

	value           proto.ColStr
	valueNormalized proto.ColStr
}

func newLabelsColumns() *labelsColumns {
	return &labelsColumns{
		name:           new(proto.ColStr).LowCardinality(),
		nameNormalized: new(proto.ColStr).LowCardinality(),
	}
}

func (c *labelsColumns) Columns() Columns {
	return Columns{
		{Name: "name", Data: c.name},
		{Name: "name_normalized", Data: c.nameNormalized},
		{Name: "value", Data: &c.value},
		{Name: "value_normalized", Data: &c.valueNormalized},
	}
}
func (c *labelsColumns) Input() proto.Input    { return c.Columns().Input() }
func (c *labelsColumns) Result() proto.Results { return c.Columns().Result() }

type exemplarColumns struct {
	name           *proto.ColLowCardinality[string]
	nameNormalized *proto.ColLowCardinality[string]
	timestamp      *proto.ColDateTime64

	filteredAttributes proto.ColStr
	exemplarTimestamp  *proto.ColDateTime64
	value              proto.ColFloat64
	spanID             proto.ColFixedStr8
	traceID            proto.ColFixedStr16

	attributes *Attributes
	resource   *Attributes
}

func newExemplarColumns() *exemplarColumns {
	return &exemplarColumns{
		name:              new(proto.ColStr).LowCardinality(),
		nameNormalized:    new(proto.ColStr).LowCardinality(),
		timestamp:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		exemplarTimestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		attributes:        NewAttributes(colAttrs),
		resource:          NewAttributes(colResource),
	}
}

func (c *exemplarColumns) Columns() Columns {
	return MergeColumns(Columns{
		{Name: "name", Data: c.name},
		{Name: "name_normalized", Data: c.nameNormalized},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "filtered_attributes", Data: &c.filteredAttributes},
		{Name: "exemplar_timestamp", Data: c.exemplarTimestamp},
		{Name: "value", Data: &c.value},
		{Name: "span_id", Data: &c.spanID},
		{Name: "trace_id", Data: &c.traceID},
	}, c.attributes.Columns(), c.resource.Columns())
}

func (c *exemplarColumns) Input() proto.Input    { return c.Columns().Input() }
func (c *exemplarColumns) Result() proto.Results { return c.Columns().Result() }
