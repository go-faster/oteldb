package chdump

import (
	"iter"
	"slices"

	"github.com/ClickHouse/ch-go/proto"
)

// Points is a parsed dump of metric points.
type Points struct {
	Name           *proto.ColLowCardinality[string]
	NameNormalized *proto.ColLowCardinality[string]
	Timestamp      *proto.ColDateTime64

	Mapping *proto.ColEnum8
	Value   *proto.ColFloat64

	Flags      *proto.ColUInt8
	Attributes *Attributes
	Resource   *Attributes
	Scope      *Attributes
}

// NewPoints creates a new [Points].
func NewPoints() *Points {
	c := &Points{
		Name:           new(proto.ColStr).LowCardinality(),
		NameNormalized: new(proto.ColStr).LowCardinality(),
		Timestamp:      new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		Mapping: new(proto.ColEnum8),
		Value:   new(proto.ColFloat64),

		Flags:      new(proto.ColUInt8),
		Attributes: NewAttributes(),
		Resource:   NewAttributes(),
		Scope:      NewAttributes(),
	}

	return c
}

// Result returns a [Points] result.
func (c *Points) Result() proto.Results {
	return slices.Collect(c.columns())
}

// Reset resets [Points] data.
func (c *Points) Reset() {
	for col := range c.columns() {
		col.Data.Reset()
	}
}

func (c *Points) columns() iter.Seq[proto.ResultColumn] {
	return func(yield func(proto.ResultColumn) bool) {
		for _, col := range []proto.ResultColumn{
			{Name: "name", Data: c.Name},
			{Name: "name_normalized", Data: c.NameNormalized},
			{Name: "timestamp", Data: c.Timestamp},

			{Name: "mapping", Data: c.Mapping},
			{Name: "value", Data: c.Value},

			{Name: "flags", Data: c.Flags},
			{Name: "attribute", Data: c.Attributes},
			{Name: "resource", Data: c.Resource},
			{Name: "scope", Data: c.Scope},
		} {
			if !yield(col) {
				return
			}
		}
	}
}

// ExpHistograms is a parsed dump of metric ExpHistograms.
type ExpHistograms struct {
	Name           *proto.ColLowCardinality[string]
	NameNormalized *proto.ColLowCardinality[string]
	Timestamp      *proto.ColDateTime64

	Count                *proto.ColUInt64
	Sum                  *proto.ColNullable[float64]
	Min                  *proto.ColNullable[float64]
	Max                  *proto.ColNullable[float64]
	Scale                *proto.ColInt32
	ZeroCount            *proto.ColUInt64
	PositiveOffset       *proto.ColInt32
	PositiveBucketCounts *proto.ColArr[uint64]
	NegativeOffset       *proto.ColInt32
	NegativeBucketCounts *proto.ColArr[uint64]

	Flags      *proto.ColUInt8
	Attributes *Attributes
	Scope      *Attributes
	Resource   *Attributes
}

// NewExpHistograms creates a new [ExpHistograms].
func NewExpHistograms() *ExpHistograms {
	c := &ExpHistograms{
		Name:           new(proto.ColStr).LowCardinality(),
		NameNormalized: new(proto.ColStr).LowCardinality(),
		Timestamp:      new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		Count:                new(proto.ColUInt64),
		Sum:                  new(proto.ColFloat64).Nullable(),
		Min:                  new(proto.ColFloat64).Nullable(),
		Max:                  new(proto.ColFloat64).Nullable(),
		Scale:                new(proto.ColInt32),
		ZeroCount:            new(proto.ColUInt64),
		PositiveOffset:       new(proto.ColInt32),
		PositiveBucketCounts: new(proto.ColUInt64).Array(),
		NegativeOffset:       new(proto.ColInt32),
		NegativeBucketCounts: new(proto.ColUInt64).Array(),

		Flags:      new(proto.ColUInt8),
		Scope:      NewAttributes(),
		Attributes: NewAttributes(),
		Resource:   NewAttributes(),
	}

	return c
}

// Result returns a [ExpHistograms] result.
func (c *ExpHistograms) Result() proto.Results {
	return slices.Collect(c.columns())
}

// Reset resets [ExpHistograms] data.
func (c *ExpHistograms) Reset() {
	for col := range c.columns() {
		col.Data.Reset()
	}
}

func (c *ExpHistograms) columns() iter.Seq[proto.ResultColumn] {
	return func(yield func(proto.ResultColumn) bool) {
		for _, col := range []proto.ResultColumn{
			{Name: "name", Data: c.Name},
			{Name: "name_normalized", Data: c.NameNormalized},
			{Name: "timestamp", Data: c.Timestamp},

			{Name: "exp_histogram_count", Data: c.Count},
			{Name: "exp_histogram_sum", Data: c.Sum},
			{Name: "exp_histogram_min", Data: c.Min},
			{Name: "exp_histogram_max", Data: c.Max},
			{Name: "exp_histogram_scale", Data: c.Scale},
			{Name: "exp_histogram_zerocount", Data: c.ZeroCount},
			{Name: "exp_histogram_positive_offset", Data: c.PositiveOffset},
			{Name: "exp_histogram_positive_bucket_counts", Data: c.PositiveBucketCounts},
			{Name: "exp_histogram_negative_offset", Data: c.NegativeOffset},
			{Name: "exp_histogram_negative_bucket_counts", Data: c.NegativeBucketCounts},

			{Name: "flags", Data: c.Flags},
			{Name: "attribute", Data: c.Attributes},
			{Name: "scope", Data: c.Scope},
			{Name: "resource", Data: c.Resource},
		} {
			if !yield(col) {
				return
			}
		}
	}
}
