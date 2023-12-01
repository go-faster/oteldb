package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"
)

type pointColumns struct {
	name      *proto.ColLowCardinality[string]
	timestamp *proto.ColDateTime64

	mapping proto.ColEnum8
	value   proto.ColFloat64

	flags      proto.ColUInt32
	attributes proto.ColStr
	resource   proto.ColStr
}

func newPointColumns() *pointColumns {
	return &pointColumns{
		name:      new(proto.ColStr).LowCardinality(),
		timestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
	}
}

func (c *pointColumns) Input() proto.Input {
	input := proto.Input{
		{Name: "name", Data: c.name},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "mapping", Data: proto.Wrap(&c.mapping, metricMappingDDL)},
		{Name: "value", Data: c.value},

		{Name: "flags", Data: c.flags},
		{Name: "attributes", Data: c.attributes},
		{Name: "resource", Data: c.resource},
	}
	return input
}

func (c *pointColumns) Result() proto.Results {
	return proto.Results{
		{Name: "name", Data: c.name},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "mapping", Data: &c.mapping},
		{Name: "value", Data: &c.value},

		{Name: "flags", Data: &c.flags},
		{Name: "attributes", Data: &c.attributes},
		{Name: "resource", Data: &c.resource},
	}
}

type histogramColumns struct {
	name      *proto.ColLowCardinality[string]
	timestamp *proto.ColDateTime64

	count          proto.ColUInt64
	sum            *proto.ColNullable[float64]
	min            *proto.ColNullable[float64]
	max            *proto.ColNullable[float64]
	bucketCounts   *proto.ColArr[uint64]
	explicitBounds *proto.ColArr[float64]

	flags      proto.ColUInt32
	attributes proto.ColStr
	resource   proto.ColStr
}

func newHistogramColumns() *histogramColumns {
	return &histogramColumns{
		name:      new(proto.ColStr).LowCardinality(),
		timestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		sum:            new(proto.ColFloat64).Nullable(),
		min:            new(proto.ColFloat64).Nullable(),
		max:            new(proto.ColFloat64).Nullable(),
		bucketCounts:   new(proto.ColUInt64).Array(),
		explicitBounds: new(proto.ColFloat64).Array(),
	}
}

func (c *histogramColumns) Input() proto.Input {
	input := proto.Input{
		{Name: "name", Data: c.name},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "histogram_count", Data: c.count},
		{Name: "histogram_sum", Data: c.sum},
		{Name: "histogram_min", Data: c.min},
		{Name: "histogram_max", Data: c.max},
		{Name: "histogram_bucket_counts", Data: c.bucketCounts},
		{Name: "histogram_explicit_bounds", Data: c.explicitBounds},

		{Name: "flags", Data: c.flags},
		{Name: "attributes", Data: c.attributes},
		{Name: "resource", Data: c.resource},
	}
	return input
}

func (c *histogramColumns) Result() proto.Results {
	return proto.Results{
		{Name: "name", Data: c.name},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "histogram_count", Data: &c.count},
		{Name: "histogram_sum", Data: c.sum},
		{Name: "histogram_min", Data: c.min},
		{Name: "histogram_max", Data: c.max},
		{Name: "histogram_bucket_counts", Data: c.bucketCounts},
		{Name: "histogram_explicit_bounds", Data: c.explicitBounds},

		{Name: "flags", Data: &c.flags},
		{Name: "attributes", Data: &c.attributes},
		{Name: "resource", Data: &c.resource},
	}
}

type expHistogramColumns struct {
	name      *proto.ColLowCardinality[string]
	timestamp *proto.ColDateTime64

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
	attributes proto.ColStr
	resource   proto.ColStr
}

func newExpHistogramColumns() *expHistogramColumns {
	return &expHistogramColumns{
		name:      new(proto.ColStr).LowCardinality(),
		timestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		sum:                  new(proto.ColFloat64).Nullable(),
		min:                  new(proto.ColFloat64).Nullable(),
		max:                  new(proto.ColFloat64).Nullable(),
		positiveBucketCounts: new(proto.ColUInt64).Array(),
		negativeBucketCounts: new(proto.ColUInt64).Array(),
	}
}

func (c *expHistogramColumns) Input() proto.Input {
	input := proto.Input{
		{Name: "name", Data: c.name},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "exp_histogram_count", Data: c.count},
		{Name: "exp_histogram_sum", Data: c.sum},
		{Name: "exp_histogram_min", Data: c.min},
		{Name: "exp_histogram_max", Data: c.max},
		{Name: "exp_histogram_scale", Data: c.scale},
		{Name: "exp_histogram_zerocount", Data: c.zerocount},
		{Name: "exp_histogram_positive_offset", Data: c.positiveOffset},
		{Name: "exp_histogram_positive_bucket_counts", Data: c.positiveBucketCounts},
		{Name: "exp_histogram_negative_offset", Data: c.negativeOffset},
		{Name: "exp_histogram_negative_bucket_counts", Data: c.negativeBucketCounts},

		{Name: "flags", Data: c.flags},
		{Name: "attributes", Data: c.attributes},
		{Name: "resource", Data: c.resource},
	}
	return input
}

func (c *expHistogramColumns) Result() proto.Results {
	return proto.Results{
		{Name: "name", Data: c.name},
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
		{Name: "attributes", Data: &c.attributes},
		{Name: "resource", Data: &c.resource},
	}
}

type summaryColumns struct {
	name      *proto.ColLowCardinality[string]
	timestamp *proto.ColDateTime64

	count     proto.ColUInt64
	sum       proto.ColFloat64
	quantiles *proto.ColArr[float64]
	values    *proto.ColArr[float64]

	flags      proto.ColUInt32
	attributes proto.ColStr
	resource   proto.ColStr
}

func newSummaryColumns() *summaryColumns {
	return &summaryColumns{
		name:      new(proto.ColStr).LowCardinality(),
		timestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		quantiles: new(proto.ColFloat64).Array(),
		values:    new(proto.ColFloat64).Array(),
	}
}

func (c *summaryColumns) Input() proto.Input {
	input := proto.Input{
		{Name: "name", Data: c.name},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "summary_count", Data: c.count},
		{Name: "summary_sum", Data: c.sum},
		{Name: "summary_quantiles", Data: c.quantiles},
		{Name: "summary_values", Data: c.values},

		{Name: "flags", Data: c.flags},
		{Name: "attributes", Data: c.attributes},
		{Name: "resource", Data: c.resource},
	}
	return input
}

func (c *summaryColumns) Result() proto.Results {
	return proto.Results{
		{Name: "name", Data: c.name},
		{Name: "timestamp", Data: c.timestamp},

		{Name: "summary_count", Data: &c.count},
		{Name: "summary_sum", Data: &c.sum},
		{Name: "summary_quantiles", Data: c.quantiles},
		{Name: "summary_values", Data: c.values},

		{Name: "flags", Data: &c.flags},
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
