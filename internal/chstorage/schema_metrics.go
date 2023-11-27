package chstorage

import (
	"errors"
	"strconv"

	"github.com/go-faster/jx"
)

const (
	pointsSchema = `CREATE TABLE IF NOT EXISTS %s
	(
		name LowCardinality(String),
		timestamp DateTime64(9),

		value Float64,

		flags	UInt32,
		attributes	String,
		resource	String
	)
	ENGINE = MergeTree()
	ORDER BY timestamp;`
	histogramsSchema = `CREATE TABLE IF NOT EXISTS %s
	(
		name LowCardinality(String),
		timestamp DateTime64(9),

		histogram_count UInt64,
		histogram_sum Nullable(Float64),
		histogram_min Nullable(Float64),
		histogram_max Nullable(Float64),
		histogram_bucket_counts Array(UInt64),
		histogram_explicit_bounds Array(Float64),

		flags	UInt32,
		attributes	String,
		resource	String
	)
	ENGINE = MergeTree()
	ORDER BY timestamp;`
	expHistogramsSchema = `CREATE TABLE IF NOT EXISTS %s
	(
		name LowCardinality(String),
		timestamp DateTime64(9),

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
		attributes	String,
		resource	String
	)
	ENGINE = MergeTree()
	ORDER BY timestamp;`
	summariesSchema = `CREATE TABLE IF NOT EXISTS %s
	(
		name LowCardinality(String),
		timestamp DateTime64(9),

		summary_count UInt64,
		summary_sum Float64,
		summary_quantiles Array(Float64),
		summary_values Array(Float64),

		flags	UInt32,
		attributes	String,
		resource	String
	)
	ENGINE = MergeTree()
	ORDER BY timestamp;`

	labelsSchema = `CREATE TABLE IF NOT EXISTS %s
	(
		name LowCardinality(String),
		value String
	)
	ENGINE = MergeTree()
	PRIMARY KEY (name);`
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
