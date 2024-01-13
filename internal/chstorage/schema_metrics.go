package chstorage

import (
	"errors"
	"strconv"

	"github.com/go-faster/jx"
)

type metricMapping int8

const (
	noMapping metricMapping = iota
	histogramCount
	histogramSum
	histogramMin
	histogramMax
	histogramBucket
	summaryCount
	summarySum
	summaryQuantile
)

const (
	pointsSchema = `
	(
		name LowCardinality(String),
		name_normalized LowCardinality(String),
		timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),

		mapping Enum8(` + metricMappingDDL + `) CODEC(T64),
		value Float64 CODEC(Gorilla),

		flags	    UInt8  CODEC(T64),

		attributes         Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
		attributes_hash    FixedString(16),
		attributes_types   Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]
		resource           Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
		resource_types     Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]
		resource_hash      FixedString(16),

		INDEX idx_ts timestamp TYPE minmax GRANULARITY 8192,

	    INDEX idx_keys arrayConcat(mapKeys(attributes), mapKeys(resource))       TYPE bloom_filter(0.01) GRANULARITY 1,
	    INDEX idx_values arrayConcat(mapValues(attributes), mapValues(resource)) TYPE bloom_filter(0.01) GRANULARITY 1,
	)
	ENGINE = MergeTree()
	PARTITION BY toYYYYMMDD(timestamp)
	PRIMARY KEY (name_normalized, mapping, resource_hash, attributes_hash)
	ORDER BY (name_normalized, mapping, resource_hash, attributes_hash, timestamp)`
	metricMappingDDL = `
		'NO_MAPPING' = 0,
		'HISTOGRAM_COUNT' = 1,
		'HISTOGRAM_SUM' = 2,
		'HISTOGRAM_MIN' = 3,
		'HISTOGRAM_MAX' = 4,
		'HISTOGRAM_BUCKET' = 5,
		'SUMMARY_COUNT' = 6,
		'SUMMARY_SUM' = 7,
		'SUMMARY_QUANTILE' = 8
		`
	expHistogramsSchema = `
	(
		name LowCardinality(String),
		name_normalized LowCardinality(String),
		timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),

		exp_histogram_count UInt64,
		exp_histogram_sum Nullable(Float64),
		exp_histogram_min Nullable(Float64),
		exp_histogram_max Nullable(Float64),
		exp_histogram_scale Int32,
		exp_histogram_zerocount UInt64,
		exp_histogram_positive_offset Int32,
		exp_histogram_positive_bucket_counts Array(UInt64),
		exp_histogram_negative_offset Int32,
		exp_histogram_negative_bucket_counts Array(UInt64),

		flags	UInt32,
		attributes         Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
		attributes_hash    FixedString(16),
		attributes_types   Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]
		resource           Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
		resource_types     Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]
		resource_hash      FixedString(16),
	)
	ENGINE = MergeTree()
	ORDER BY timestamp`
	exemplarsSchema = `
	(
		name LowCardinality(String),
		name_normalized LowCardinality(String),
		timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),

		filtered_attributes String,
		exemplar_timestamp DateTime(9) CODEC(Delta, ZSTD(1)),
		value Float64,
		span_id FixedString(8),
		trace_id FixedString(16),

		attributes         Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
		attributes_hash    FixedString(16),
		attributes_types   Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]
		resource           Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
		resource_types     Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]
		resource_hash      FixedString(16),
	)
	ENGINE = MergeTree()
	ORDER BY (name_normalized, resource_hash, attributes_hash, timestamp)`

	labelsSchema = `
	(
		name            LowCardinality(String), -- original name, i.e. 'foo.bar'
		name_normalized LowCardinality(String), -- normalized name, 'foo_bar'

		value            String, -- original value, 'foo.bar'
		value_normalized String  -- normalized value, 'foo_bar' or empty, if original already normalized
	)
	ENGINE = ReplacingMergeTree
	ORDER BY (name_normalized, value)`
)

func parseLabels(s string, to map[string]string) error {
	d := jx.DecodeStr(s)
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		switch d.Next() {
		case jx.String:
			val, err := d.Str()
			if err != nil {
				return err
			}
			to[string(key)] = val
			return nil
		case jx.Number:
			val, err := d.Num()
			if err != nil {
				return err
			}
			to[string(key)] = val.String()
			return nil
		case jx.Null:
			return d.Null()
		case jx.Bool:
			val, err := d.Bool()
			if err != nil {
				return err
			}
			to[string(key)] = strconv.FormatBool(val)
			return nil
		case jx.Array, jx.Object:
			val, err := d.Raw()
			if err != nil {
				return err
			}
			to[string(key)] = val.String()
			return nil
		default:
			return errors.New("invalid type")
		}
	})
}
