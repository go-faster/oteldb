package tracestorage

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.ytsaurus.tech/yt/go/yson"

	"github.com/go-faster/errors"
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
	w.BeginList()
	t := attr.Type()

	w.Int64(int64(t))
	switch t {
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		w.String("")
	default:
		w.String(attr.AsString())
	}

	switch t {
	case pcommon.ValueTypeStr:
		w.String(attr.Str())
	case pcommon.ValueTypeBool:
		w.Bool(attr.Bool())
	case pcommon.ValueTypeInt:
		w.Int64(attr.Int())
	case pcommon.ValueTypeDouble:
		w.Float64(attr.Double())
	case pcommon.ValueTypeMap:
		otelMapToYSON(w, attr.Map())
	case pcommon.ValueTypeSlice:
		w.BeginList()
		s := attr.Slice()
		for i := 0; i < s.Len(); i++ {
			otelValueToYSON(w, s.At(i))
		}
		w.EndList()
	case pcommon.ValueTypeBytes:
		w.Bytes(attr.Bytes().AsRaw())
	}
	w.EndList()
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
	if err := ysonNext(r, yson.EventBeginList, true); err != nil {
		return err
	}

	switch ok, err := r.NextListItem(); {
	case err != nil:
		return err
	case !ok:
		return errors.New("attr has no type")
	}
	if err := consumeYsonLiteral(r, yson.TypeInt64); err != nil {
		return err
	}
	t := pcommon.ValueType(r.Int64())

	switch ok, err := r.NextListItem(); {
	case err != nil:
		return err
	case !ok:
		return errors.New("attr has no stringified value")
	}
	// Just consume it, we don't need it anyway.
	if _, err := r.NextRawValue(); err != nil {
		return err
	}

	switch ok, err := r.NextListItem(); {
	case err != nil:
		return err
	case !ok:
		return errors.New("attr has no value")
	}
	switch t {
	case pcommon.ValueTypeStr:
		if err := consumeYsonLiteral(r, yson.TypeString); err != nil {
			return err
		}
		val.SetStr(r.String())
	case pcommon.ValueTypeBool:
		if err := consumeYsonLiteral(r, yson.TypeBool); err != nil {
			return err
		}
		val.SetBool(r.Bool())
	case pcommon.ValueTypeInt:
		if err := consumeYsonLiteral(r, yson.TypeInt64); err != nil {
			return err
		}
		val.SetInt(r.Int64())
	case pcommon.ValueTypeDouble:
		if err := consumeYsonLiteral(r, yson.TypeFloat64); err != nil {
			return err
		}
		val.SetDouble(r.Float64())
	case pcommon.ValueTypeMap:
		if err := ysonToOTELMap(r, val.SetEmptyMap()); err != nil {
			return err
		}
	case pcommon.ValueTypeSlice:
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

		if err := ysonNext(r, yson.EventEndList, true); err != nil {
			return err
		}
	case pcommon.ValueTypeBytes:
		if err := consumeYsonLiteral(r, yson.TypeString); err != nil {
			return err
		}
		val.SetEmptyBytes().Append(r.Bytes()...)
	default:
		return errors.Errorf("unexpected type %d", t)
	}
	return ysonNext(r, yson.EventEndList, true)
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
	ysonEventName := func(e yson.Event) string {
		switch e {
		case yson.EventBeginList:
			return "EventBeginList"
		case yson.EventEndList:
			return "EventEndList"
		case yson.EventBeginAttrs:
			return "EventBeginAttrs"
		case yson.EventEndAttrs:
			return "EventEndAttrs"
		case yson.EventBeginMap:
			return "EventBeginMap"
		case yson.EventEndMap:
			return "EventEndMap"
		case yson.EventLiteral:
			return "EventLiteral"
		case yson.EventKey:
			return "EventKey"
		case yson.EventEOF:
			return "EventEOF"
		default:
			return "Unknown"
		}
	}

	switch got, err := r.Next(attrs); {
	case err != nil:
		return err
	case expect != got:
		return errors.Errorf("expected event %s, got %s", ysonEventName(expect), ysonEventName(got))
	default:
		return nil
	}
}
