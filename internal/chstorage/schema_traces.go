package chstorage

import (
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

const (
	spansSchema = `CREATE TABLE IF NOT EXISTS %s
(
	trace_id UUID,
	span_id UInt64,
	trace_state String,
	parent_span_id UInt64,
	name LowCardinality(String),
	kind Enum8(` + kindDDL + `),
	start DateTime64(9),
	end DateTime64(9),
	status_code Int32,
	status_message String,

	batch_id UUID,
	attributes String,
	resource String,

	scope_name String,
	scope_version String,
	scope_attributes String,

	events_timestamps Array(DateTime64(9)),
	events_names Array(String),
	events_attributes Array(String),

	links_trace_ids Array(UUID),
	links_span_ids Array(UInt64),
	links_tracestates Array(String),
	links_attributes Array(String)
)
ENGINE = MergeTree()
PRIMARY KEY (trace_id, span_id);`
	kindDDL    = `'KIND_UNSPECIFIED' = 0,'KIND_INTERNAL' = 1,'KIND_SERVER' = 2,'KIND_CLIENT' = 3,'KIND_PRODUCER' = 4,'KIND_CONSUMER' = 5`
	tagsSchema = `CREATE TABLE IF NOT EXISTS %s
	(
		name LowCardinality(String),
		value String,
		value_type Enum8(` + valueTypeDDL + `)
	)
	ENGINE = ReplacingMergeTree
	ORDER BY (value_type, name, value);`
	valueTypeDDL = `'EMPTY' = 0,'STR' = 1,'INT' = 2,'DOUBLE' = 3,'BOOL' = 4,'MAP' = 5,'SLICE' = 6,'BYTES' = 7`
)

func encodeAttributes(attrs pcommon.Map, additional ...[2]string) string {
	e := jx.GetEncoder()
	defer jx.PutEncoder(e)

	encodeMap(e, attrs, additional...)
	return e.String()
}

func encodeValue(e *jx.Encoder, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		e.Str(v.Str())
	case pcommon.ValueTypeInt:
		e.Int64(v.Int())
	case pcommon.ValueTypeDouble:
		e.Float64(v.Double())
	case pcommon.ValueTypeBool:
		e.Bool(v.Bool())
	case pcommon.ValueTypeMap:
		m := v.Map()
		encodeMap(e, m)
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		e.ArrStart()
		for i := 0; i < s.Len(); i++ {
			encodeValue(e, s.At(i))
		}
		e.ArrEnd()
	case pcommon.ValueTypeBytes:
		e.ByteStr(v.Bytes().AsRaw())
	default:
		e.Null()
	}
}

func encodeMap(e *jx.Encoder, m pcommon.Map, additional ...[2]string) {
	if otelstorage.Attrs(m).IsZero() && len(additional) == 0 {
		e.ObjEmpty()
		return
	}
	e.ObjStart()
	m.Range(func(k string, v pcommon.Value) bool {
		e.FieldStart(k)
		encodeValue(e, v)
		return true
	})
	for _, pair := range additional {
		e.FieldStart(pair[0])
		e.Str(pair[1])
	}
	e.ObjEnd()
}

func decodeAttributes(s string) (otelstorage.Attrs, error) {
	result := pcommon.NewMap()
	err := decodeMap(jx.DecodeStr(s), result)
	return otelstorage.Attrs(result), err
}

func decodeValue(d *jx.Decoder, val pcommon.Value) error {
	switch d.Next() {
	case jx.String:
		v, err := d.Str()
		if err != nil {
			return err
		}
		val.SetStr(v)
		return nil
	case jx.Number:
		n, err := d.Num()
		if err != nil {
			return err
		}
		if n.IsInt() {
			v, err := n.Int64()
			if err != nil {
				return err
			}
			val.SetInt(v)
		} else {
			v, err := n.Float64()
			if err != nil {
				return err
			}
			val.SetDouble(v)
		}
		return nil
	case jx.Null:
		if err := d.Null(); err != nil {
			return err
		}
		// Do nothing, keep value empty.
		return nil
	case jx.Bool:
		v, err := d.Bool()
		if err != nil {
			return err
		}
		val.SetBool(v)
		return nil
	case jx.Array:
		s := val.SetEmptySlice()
		return d.Arr(func(d *jx.Decoder) error {
			rval := s.AppendEmpty()
			return decodeValue(d, rval)
		})
	case jx.Object:
		m := val.SetEmptyMap()
		return decodeMap(d, m)
	default:
		return d.Skip()
	}
}

func decodeMap(d *jx.Decoder, m pcommon.Map) error {
	return d.Obj(func(d *jx.Decoder, key string) error {
		rval := m.PutEmpty(key)
		return decodeValue(d, rval)
	})
}
