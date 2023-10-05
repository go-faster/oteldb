package otelstorage

import (
	"github.com/go-faster/errors"
	"go.ytsaurus.tech/yt/go/yson"
)

// Hash is an attribute hash.
type Hash [16]byte

// MarshalYSON implemenets yson.StreamMarshaler.
func (h Hash) MarshalYSON(w *yson.Writer) error {
	w.Bytes(h[:])
	return nil
}

// UnmarshalYSON implemenets yson.StreamUnmarshaler.
func (h *Hash) UnmarshalYSON(r *yson.Reader) error {
	if h == nil {
		return errors.Errorf("can't unmarshal to %#v", h)
	}
	if err := consumeYsonLiteral(r, yson.TypeString); err != nil {
		return err
	}
	data := r.Bytes()

	const expectedLen = len(Hash{})
	if got := len(data); expectedLen != got {
		return errors.Errorf("expected %d bytes, got %d", expectedLen, got)
	}

	copy(h[:], data)
	return nil
}
