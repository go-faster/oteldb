package chstorage

import (
	"errors"
	"strconv"

	"github.com/go-faster/jx"
)

const (
	pointsSchema = `CREATE TABLE IF NOT EXISTS %s
	(
		name		LowCardinality(String),
		timestamp	DateTime64(9),
		value		Float64,
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
