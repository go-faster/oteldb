package chstorage

import (
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

type lazyAttributes struct {
	orig pcommon.Map
}

func (l *lazyAttributes) Attributes(additional ...[2]string) otelstorage.Attrs {
	if len(additional) > 0 {
		target := pcommon.NewMap()
		l.orig.CopyTo(target)
		for _, pair := range additional {
			target.PutStr(pair[0], pair[1])
		}
		return otelstorage.Attrs(target)
	}
	return otelstorage.Attrs(l.orig)
}

func encodeAttributes(attrs pcommon.Map, additional ...[2]string) []byte {
	e := &jx.Encoder{}
	encodeMap(e, attrs, additional...)
	return e.Bytes()
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

func decodeAttributes(s []byte) (otelstorage.Attrs, error) {
	result := pcommon.NewMap()
	if len(s) == 0 {
		return otelstorage.Attrs(result), nil
	}
	err := decodeMap(jx.DecodeBytes(s), result)
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
