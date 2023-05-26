package ytstore

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.ytsaurus.tech/yt/go/yson"
)

// Attrs is a YSON wrapper for attributes.
type Attrs pcommon.Map

var (
	_ yson.StreamMarshaler   = Attrs{}
	_ yson.StreamUnmarshaler = (*Attrs)(nil)
)

// MarshalYSON implemenets yson.StreamMarshaler.
func (m Attrs) MarshalYSON(w *yson.Writer) error {
	otelMapToYSON(w, m.AsMap())
	return nil
}

// AsMap returns Attrs as pcommon.Map.
func (m Attrs) AsMap() pcommon.Map {
	return pcommon.Map(m)
}

// CopyTo copies all attributes from m to target.
func (m Attrs) CopyTo(target pcommon.Map) {
	m.AsMap().CopyTo(target)
}

// UnmarshalYSON implemenets yson.StreamUnmarshaler.
func (m *Attrs) UnmarshalYSON(r *yson.Reader) error {
	nm := pcommon.NewMap()
	*m = Attrs(nm)
	return ysonToOTELMap(r, nm)
}

func otelMapToYSON(w *yson.Writer, kv pcommon.Map) {
	w.BeginMap()
	kv.Range(func(k string, v pcommon.Value) bool {
		w.MapKeyString(k)
		otelValueToYSON(w, v)
		return true
	})
	w.EndMap()
}

func otelValueToYSON(w *yson.Writer, attr pcommon.Value) {
	w.BeginMap()
	switch attr.Type() {
	case pcommon.ValueTypeStr:
		w.MapKeyString("stringValue")
		w.String(attr.Str())
	case pcommon.ValueTypeBool:
		w.MapKeyString("boolValue")
		w.Bool(attr.Bool())
	case pcommon.ValueTypeInt:
		w.MapKeyString("intValue")
		w.Int64(attr.Int())
	case pcommon.ValueTypeDouble:
		w.MapKeyString("doubleValue")
		w.Float64(attr.Double())
	case pcommon.ValueTypeMap:
		w.MapKeyString("kvlistValue")
		otelMapToYSON(w, attr.Map())
	case pcommon.ValueTypeSlice:
		w.MapKeyString("arrayValue")
		w.BeginList()
		s := attr.Slice()
		for i := 0; i < s.Len(); i++ {
			otelValueToYSON(w, s.At(i))
		}
		w.EndList()
	case pcommon.ValueTypeBytes:
		w.MapKeyString("bytesValue")
		w.Bytes(attr.Bytes().AsRaw())
	}
	w.EndMap()
}

func ysonToOTELMap(r *yson.Reader, kv pcommon.Map) error {
	if err := ysonNext(r, yson.EventBeginMap, true); err != nil {
		return err
	}

	for {
		ok, err := r.NextKey()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		key := r.String()

		val := kv.PutEmpty(key)
		if err := ysonToOTELValue(r, val); err != nil {
			return err
		}
	}

	return ysonNext(r, yson.EventEndMap, true)
}

func ysonToOTELValue(r *yson.Reader, val pcommon.Value) error {
	if err := ysonNext(r, yson.EventBeginMap, true); err != nil {
		return err
	}

	switch ok, err := r.NextKey(); {
	case err != nil:
		return err
	case !ok:
		return errors.New("value has no key")
	}
	key := r.String()

	switch key {
	case "stringValue":
		if err := consumeYsonLiteral(r, yson.TypeString); err != nil {
			return err
		}
		val.SetStr(r.String())
	case "boolValue":
		if err := consumeYsonLiteral(r, yson.TypeBool); err != nil {
			return err
		}
		val.SetBool(r.Bool())
	case "intValue":
		if err := consumeYsonLiteral(r, yson.TypeInt64); err != nil {
			return err
		}
		val.SetInt(r.Int64())
	case "doubleValue":
		if err := consumeYsonLiteral(r, yson.TypeFloat64); err != nil {
			return err
		}
		val.SetDouble(r.Float64())
	case "kvlistValue":
		if err := ysonToOTELMap(r, val.SetEmptyMap()); err != nil {
			return err
		}
	case "arrayValue":
		if err := ysonNext(r, yson.EventBeginList, true); err != nil {
			return err
		}

		s := val.SetEmptySlice()
		for {
			ok, err := r.NextListItem()
			if err != nil {
				return err
			}
			if !ok {
				break
			}

			if err := ysonToOTELValue(r, s.AppendEmpty()); err != nil {
				return err
			}
		}

		return ysonNext(r, yson.EventEndList, true)
	case "bytesValue":
		if err := consumeYsonLiteral(r, yson.TypeString); err != nil {
			return err
		}
		val.SetEmptyBytes().Append(r.Bytes()...)
	default:
		return errors.Errorf("unexpected field %q", key)
	}
	return ysonNext(r, yson.EventEndMap, true)
}

func consumeYsonLiteral(r *yson.Reader, typ yson.Type) error {
	if err := ysonNext(r, yson.EventLiteral, false); err != nil {
		return err
	}
	if got := r.Type(); got != typ {
		return errors.Errorf("expected typ %s, got %s", typ, got)
	}
	return nil
}

func ysonNext(r *yson.Reader, expect yson.Event, attrs bool) error {
	switch got, err := r.Next(attrs); {
	case err != nil:
		return err
	case expect != got:
		return errors.Errorf("expected event %v, got %v", expect, got)
	default:
		return nil
	}
}
