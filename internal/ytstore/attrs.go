package ytstore

import (
	"reflect"

	"github.com/go-faster/errors"
	"go.ytsaurus.tech/yt/go/yson"
)

// KeyValue is an Attrs entry.
type KeyValue[V any] struct {
	Key   string `yson:"key"`
	Value V      `yson:"value"`
}

var (
	_ yson.StreamUnmarshaler = (*KeyValue[int64])(nil)
	_ yson.StreamMarshaler   = KeyValue[int64]{}
)

// UnmarshalYSON implements yson.StreamUnmarshaler.
func (kv *KeyValue[V]) UnmarshalYSON(r *yson.Reader) error {
	switch e, err := r.Next(true); {
	case err != nil:
		return err
	case e != yson.EventBeginList:
		return &yson.TypeError{UserType: reflect.TypeOf(kv), YSONType: r.Type()}
	}

	{
		if ok, err := r.NextListItem(); err != nil {
			return err
		} else if !ok {
			return errors.New("missing key")
		}

		switch e, err := r.Next(true); {
		case err != nil:
			return err
		case e != yson.EventLiteral:
			return &yson.TypeError{UserType: reflect.TypeOf(kv), YSONType: r.Type()}
		}
		kv.Key = r.String()
	}
	{
		if ok, err := r.NextListItem(); err != nil {
			return err
		} else if !ok {
			return errors.Errorf("missing value for key %q", kv.Key)
		}

		raw, err := r.NextRawValue()
		if err != nil {
			return errors.Wrap(err, "get value")
		}
		if err := yson.Unmarshal(raw, &kv.Value); err != nil {
			return errors.Wrap(err, "unmarshal value")
		}
	}

	switch e, err := r.Next(false); {
	case err != nil:
		return err
	case e != yson.EventEndList:
		panic("invalid decoder state")
	}

	return nil
}

// MarshalYSON implements yson.StreamMarshaler.
func (kv KeyValue[V]) MarshalYSON(w *yson.Writer) error {
	w.BeginList()
	w.String(kv.Key)
	w.Any(kv.Value)
	w.EndList()
	return w.Err()
}

// Attrs represent attributes map.
type Attrs[V any] []KeyValue[V]
