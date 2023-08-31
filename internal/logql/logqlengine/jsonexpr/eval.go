package jsonexpr

import (
	"strconv"

	"github.com/go-faster/jx"

	"github.com/go-faster/oteldb/internal/logql"
)

// Extract extracts values from given paths using extract callback.
func Extract(
	d *jx.Decoder,
	paths map[logql.Label]Path,
	extract func(logql.Label, string),
) error {
	e := extractor{
		// TODO(tdakkota): re-use slice?
		current: make(Path, 0, 16),
		paths:   paths,
		extract: extract,
	}
	return e.walk(d)
}

// extractor extracts values from JSON value.
type extractor struct {
	current Path
	paths   map[logql.Label]Path
	extract func(logql.Label, string)
}

func (e *extractor) walk(d *jx.Decoder) error {
	switch d.Next() {
	case jx.String:
		val, err := d.Str()
		if err != nil {
			return err
		}
		e.matchLiteral(val)
		return nil
	case jx.Number:
		num, err := d.Num()
		if err != nil {
			return err
		}
		e.matchLiteral(string(num))
		return nil
	case jx.Null:
		if err := d.Null(); err != nil {
			return err
		}
		e.matchLiteral("")
		return nil
	case jx.Bool:
		val, err := d.Bool()
		if err != nil {
			return err
		}
		e.matchLiteral(strconv.FormatBool(val))
		return nil
	case jx.Array:
		return e.walkArr(d)
	case jx.Object:
		return e.walkObj(d)
	default:
		return d.Skip()
	}
}

func (e *extractor) walkObj(d *jx.Decoder) error {
	if err := e.tryMatchRaw(d); err != nil {
		return err
	}

	return d.Obj(func(d *jx.Decoder, key string) error {
		e.current = append(e.current, KeySel(key))
		if err := e.walk(d); err != nil {
			return err
		}
		e.current = e.current[:len(e.current)-1]
		return nil
	})
}

func (e *extractor) walkArr(d *jx.Decoder) error {
	if err := e.tryMatchRaw(d); err != nil {
		return err
	}

	n := 0
	return d.Arr(func(d *jx.Decoder) error {
		e.current = append(e.current, IndexSel(n))
		if err := e.walk(d); err != nil {
			return err
		}
		e.current = e.current[:len(e.current)-1]
		n++
		return nil
	})
}

func (e *extractor) matchLiteral(val string) {
	for label, p := range e.paths {
		if e.current.Equal(p) {
			e.extract(label, val)
		}
	}
}

func (e *extractor) tryMatchRaw(d *jx.Decoder) error {
	var raw string
	for label, p := range e.paths {
		if !e.current.Equal(p) {
			// No match.
			continue
		}

		if raw == "" {
			if err := d.Capture(func(d *jx.Decoder) error {
				data, err := d.Raw()
				if err != nil {
					return err
				}
				raw = string(data)
				return nil
			}); err != nil {
				return err
			}
		}

		e.extract(label, raw)
	}
	return nil
}
