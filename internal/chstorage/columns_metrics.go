package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/ddl"
)

type pointColumns struct {
	name      *proto.ColLowCardinality[string]
	timestamp *proto.ColDateTime64

	mapping proto.ColEnum8
	value   proto.ColFloat64

	flags      proto.ColUInt8
	attributes *Attributes
	scope      *Attributes
	resource   *Attributes
}

func newPointColumns() *pointColumns {
	return &pointColumns{
		name:      new(proto.ColStr).LowCardinality(),
		timestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		attributes: NewAttributes(colAttrs),
		scope:      NewAttributes(colScope),
		resource:   NewAttributes(colResource),
	}
}

func (c *pointColumns) Columns() Columns {
	return MergeColumns(
		Columns{
			{Name: "name", Data: c.name},
			{Name: "timestamp", Data: c.timestamp},

			{Name: "mapping", Data: proto.Wrap(&c.mapping, metricMappingDDL)},
			{Name: "value", Data: &c.value},

			{Name: "flags", Data: &c.flags},
		},
		c.attributes.Columns(),
		c.scope.Columns(),
		c.resource.Columns(),
	)
}

func (c *pointColumns) Input() proto.Input                { return c.Columns().Input() }
func (c *pointColumns) Result() proto.Results             { return c.Columns().Result() }
func (c *pointColumns) ChsqlResult() []chsql.ResultColumn { return c.Columns().ChsqlResult() }

func (c *pointColumns) DDL() ddl.Table {
	table := ddl.Table{
		Engine:      "MergeTree",
		PartitionBy: "toYYYYMMDD(timestamp)",
		PrimaryKey:  []string{"name", "mapping", "resource", "attribute"},
		OrderBy:     []string{"name", "mapping", "resource", "attribute", "timestamp"},
		TTL:         ddl.TTL{Field: "timestamp"},
		Indexes: []ddl.Index{
			{
				Name:        "idx_ts",
				Target:      "timestamp",
				Type:        "minmax",
				Granularity: 8192,
			},
		},
		Columns: []ddl.Column{
			{
				Name:  "name",
				Type:  c.name.Type(),
				Codec: "ZSTD(1)",
			},
			{
				Name:  "timestamp",
				Type:  c.timestamp.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name:  "mapping",
				Type:  c.mapping.Type().Sub(metricMappingDDL),
				Codec: "T64, ZSTD(1)",
			},
			{
				Name:  "value",
				Type:  c.value.Type(),
				Codec: "Gorilla, ZSTD(1)",
			},
			{
				Name:  "flags",
				Type:  c.flags.Type(),
				Codec: "T64, ZSTD(1)",
			},
		},
	}

	c.attributes.DDL(&table)
	c.resource.DDL(&table)
	c.scope.DDL(&table)

	return table
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

	flags      proto.ColUInt8
	attributes *Attributes
	scope      *Attributes
	resource   *Attributes
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

		attributes: NewAttributes(colAttrs),
		scope:      NewAttributes(colScope),
		resource:   NewAttributes(colResource),
	}
}

func (c *expHistogramColumns) Columns() Columns {
	return MergeColumns(
		Columns{
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
		},
		c.attributes.Columns(),
		c.scope.Columns(),
		c.resource.Columns(),
	)
}

func (c *expHistogramColumns) Input() proto.Input                { return c.Columns().Input() }
func (c *expHistogramColumns) Result() proto.Results             { return c.Columns().Result() }
func (c *expHistogramColumns) ChsqlResult() []chsql.ResultColumn { return c.Columns().ChsqlResult() }

func (c *expHistogramColumns) DDL() ddl.Table {
	table := ddl.Table{
		Engine:      "MergeTree",
		PartitionBy: "toYYYYMMDD(timestamp)",
		OrderBy:     []string{"timestamp"},
		Columns: []ddl.Column{
			{
				Name:  "name",
				Type:  c.name.Type(),
				Codec: "ZSTD(1)",
			},
			{
				Name:  "timestamp",
				Type:  c.timestamp.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name: "exp_histogram_count",
				Type: c.count.Type(),
			},
			{
				Name: "exp_histogram_sum",
				Type: c.sum.Type(),
			},
			{
				Name: "exp_histogram_min",
				Type: c.min.Type(),
			},
			{
				Name: "exp_histogram_max",
				Type: c.max.Type(),
			},
			{
				Name: "exp_histogram_scale",
				Type: c.scale.Type(),
			},
			{
				Name: "exp_histogram_zerocount",
				Type: c.zerocount.Type(),
			},
			{
				Name: "exp_histogram_positive_offset",
				Type: c.positiveOffset.Type(),
			},
			{
				Name: "exp_histogram_positive_bucket_counts",
				Type: c.positiveBucketCounts.Type(),
			},
			{
				Name: "exp_histogram_negative_offset",
				Type: c.negativeOffset.Type(),
			},
			{
				Name: "exp_histogram_negative_bucket_counts",
				Type: c.negativeBucketCounts.Type(),
			},
			{
				Name:  "flags",
				Type:  c.flags.Type(),
				Codec: "T64, ZSTD(1)",
			},
		},
	}

	c.attributes.DDL(&table)
	c.resource.DDL(&table)
	c.scope.DDL(&table)

	return table
}

type labelsColumns struct {
	name  *proto.ColLowCardinality[string]
	value proto.ColStr
	scope proto.ColEnum8
}

func newLabelsColumns() *labelsColumns {
	return &labelsColumns{
		name: new(proto.ColStr).LowCardinality(),
	}
}

func (c *labelsColumns) Columns() Columns {
	return Columns{
		{Name: "name", Data: c.name},
		{Name: "value", Data: &c.value},
		{Name: "scope", Data: proto.Wrap(&c.scope, metricLabelScopeDDL)},
	}
}
func (c *labelsColumns) Input() proto.Input                { return c.Columns().Input() }
func (c *labelsColumns) Result() proto.Results             { return c.Columns().Result() }
func (c *labelsColumns) ChsqlResult() []chsql.ResultColumn { return c.Columns().ChsqlResult() }

func (c *labelsColumns) DDL() ddl.Table {
	return ddl.Table{
		Engine:  "ReplacingMergeTree",
		OrderBy: []string{"name", "value", "scope"},
		Columns: []ddl.Column{
			{
				Name: "name",
				Type: c.name.Type(),
			},
			{
				Name: "value",
				Type: c.value.Type(),
			},
			{
				Name: "scope",
				Type: c.scope.Type().Sub(metricLabelScopeDDL),
			},
		},
	}
}

type exemplarColumns struct {
	name      *proto.ColLowCardinality[string]
	timestamp *proto.ColDateTime64

	filteredAttributes proto.ColBytes
	exemplarTimestamp  *proto.ColDateTime64
	value              proto.ColFloat64
	spanID             proto.ColFixedStr8
	traceID            proto.ColFixedStr16

	attributes *Attributes
	scope      *Attributes
	resource   *Attributes
}

func newExemplarColumns() *exemplarColumns {
	return &exemplarColumns{
		name:              new(proto.ColStr).LowCardinality(),
		timestamp:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		exemplarTimestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		attributes:        NewAttributes(colAttrs),
		scope:             NewAttributes(colScope),
		resource:          NewAttributes(colResource),
	}
}

func (c *exemplarColumns) Columns() Columns {
	return MergeColumns(
		Columns{
			{Name: "name", Data: c.name},
			{Name: "timestamp", Data: c.timestamp},

			{Name: "filtered_attributes", Data: &c.filteredAttributes},
			{Name: "exemplar_timestamp", Data: c.exemplarTimestamp},
			{Name: "value", Data: &c.value},
			{Name: "span_id", Data: &c.spanID},
			{Name: "trace_id", Data: &c.traceID},
		},
		c.attributes.Columns(),
		c.scope.Columns(),
		c.resource.Columns(),
	)
}

func (c *exemplarColumns) Input() proto.Input                { return c.Columns().Input() }
func (c *exemplarColumns) Result() proto.Results             { return c.Columns().Result() }
func (c *exemplarColumns) ChsqlResult() []chsql.ResultColumn { return c.Columns().ChsqlResult() }

func (c *exemplarColumns) DDL() ddl.Table {
	table := ddl.Table{
		Engine:      "MergeTree",
		PartitionBy: "toYYYYMMDD(timestamp)",
		OrderBy:     []string{"name", "resource", "attribute", "timestamp"},
		Columns: []ddl.Column{
			{
				Name: "name",
				Type: c.name.Type(),
			},
			{
				Name:  "timestamp",
				Type:  c.timestamp.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name: "filtered_attributes",
				Type: c.filteredAttributes.Type(),
			},
			{
				Name:  "exemplar_timestamp",
				Type:  c.exemplarTimestamp.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name: "value",
				Type: c.value.Type(),
			},
			{
				Name: "trace_id",
				Type: c.traceID.Type(),
			},
			{
				Name: "span_id",
				Type: c.spanID.Type(),
			},
		},
	}

	c.attributes.DDL(&table)
	c.resource.DDL(&table)
	c.scope.DDL(&table)

	return table
}
