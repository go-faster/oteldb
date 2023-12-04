package chstorage

import (
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

const (
	spansSchema = `CREATE TABLE IF NOT EXISTS %s
(
	-- materialized fields from semantic conventions
	-- NB: They MUST NOT be present in the 'resource' field.
	service_instance_id LowCardinality(String) COMMENT 'service.instance.id',
	service_name        LowCardinality(String) COMMENT 'service.name',
	service_namespace   LowCardinality(String) COMMENT 'service.namespace',

	-- Trace Context Fields
	trace_id           FixedString(16) CODEC(ZSTD(1)),  -- TraceId
	span_id            FixedString(8)  CODEC(ZSTD(1)),  -- SpanId

	trace_state String,
	parent_span_id FixedString(8),
	name LowCardinality(String),
	kind Enum8(` + kindDDL + `),

	start DateTime64(9) CODEC(Delta, ZSTD(1)),
	end   DateTime64(9) CODEC(Delta, ZSTD(1)),
	duration_ns UInt64 Materialized toUnixTimestamp64Nano(end)-toUnixTimestamp64Nano(start) CODEC(T64, ZSTD(1)),

	status_code UInt8 CODEC(T64, ZSTD(1)),
	status_message LowCardinality(String),

	batch_id UUID,
	attributes         Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
	attributes_types   Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]
	resource           Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
	resource_types     Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]

	scope_name             LowCardinality(String),
	scope_version          LowCardinality(String),
	scope_attributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),  -- string[str | json]
	scope_attributes_types Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)),  -- string[type]

	events_timestamps Array(DateTime64(9)),
	events_names Array(String),
	events_attributes Array(String),

	links_trace_ids Array(FixedString(16)),
	links_span_ids Array(FixedString(8)),
	links_tracestates Array(String),
	links_attributes Array(String)
)
ENGINE = MergeTree()
ORDER BY (service_namespace, service_name, cityHash64(resource), start);`
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
