package logqlengine

import (
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// JSONExtractor is a JSON label extractor.
type JSONExtractor struct {
	Labels map[logql.Label]struct{}
}

func buildJSONExtractor(stage *logql.JSONExpressionParser) (Processor, error) {
	if len(stage.Exprs) > 0 {
		return nil, &UnsupportedError{Msg: "extraction expressions are not supported yet"}
	}

	e := &JSONExtractor{}
	if labels := stage.Labels; len(labels) > 0 {
		e.Labels = make(map[logql.Label]struct{}, len(labels))
		for _, label := range labels {
			e.Labels[label] = struct{}{}
		}
	}
	return e, nil
}

// Process implements Processor.
func (e *JSONExtractor) Process(_ otelstorage.Timestamp, line string, set LabelSet) (string, bool) {
	var err error
	if len(e.Labels) == 0 {
		err = e.extractAll(line, set)
	} else {
		err = e.extractSome(line, set)
	}
	if err != nil {
		set.SetError(err)
	}
	return line, true
}

func (e *JSONExtractor) extractSome(line string, set LabelSet) error {
	d := jx.DecodeStr(line)
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		value, ok, err := parseValue(d)
		if err != nil {
			return errors.Wrapf(err, "parse label %q", key)
		}
		if !ok {
			return nil
		}
		if _, ok := e.Labels[logql.Label(key)]; ok {
			// TODO(tdakkota): try string interning
			// TODO(tdakkota): probably, we can just use label name string
			// 	instead of allocating a new string every time
			set.Add(string(key), value)
		}
		return nil
	})
}

func (e *JSONExtractor) extractAll(line string, set LabelSet) error {
	d := jx.DecodeStr(line)
	return d.Obj(func(d *jx.Decoder, key string) error {
		value, ok, err := parseValue(d)
		if err != nil {
			return errors.Wrapf(err, "parse label %q", key)
		}
		if !ok {
			return nil
		}
		// TODO(tdakkota): try string interning
		set.Add(key, value)
		return nil
	})
}

func parseValue(d *jx.Decoder) (pcommon.Value, bool, error) {
	var val pcommon.Value
	switch tt := d.Next(); tt {
	case jx.String:
		str, err := d.Str()
		if err != nil {
			return val, false, err
		}
		val = pcommon.NewValueStr(str)
	case jx.Number:
		num, err := d.Num()
		if err != nil {
			return val, false, err
		}
		if num.IsInt() {
			n, err := num.Int64()
			if err != nil {
				return val, false, err
			}
			val = pcommon.NewValueInt(n)
		} else {
			n, err := num.Float64()
			if err != nil {
				return val, false, err
			}
			val = pcommon.NewValueDouble(n)
		}
	case jx.Null:
		err := d.Null()
		return val, false, err
	case jx.Bool:
		b, err := d.Bool()
		if err != nil {
			return val, false, err
		}
		val = pcommon.NewValueBool(b)
	case jx.Array:
		val = pcommon.NewValueSlice()
		slice := val.Slice()

		if err := d.Arr(func(d *jx.Decoder) error {
			elem, ok, err := parseValue(d)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			item := slice.AppendEmpty()
			elem.CopyTo(item)
			return nil
		}); err != nil {
			return val, false, err
		}
	case jx.Object:
		val = pcommon.NewValueMap()
		m := val.Map()

		if err := d.Obj(func(d *jx.Decoder, k string) error {
			elem, ok, err := parseValue(d)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			item := m.PutEmpty(k)
			elem.CopyTo(item)
			return nil
		}); err != nil {
			return val, false, err
		}
	default:
		return val, false, errors.Errorf("unexpected type %q", tt)
	}
	return val, true, nil
}
