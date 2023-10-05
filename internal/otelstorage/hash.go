package otelstorage

import (
	"cmp"
	"encoding/binary"
	"math"
	"slices"

	"github.com/go-faster/errors"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
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

// AttrHash computes attributes hash.
func AttrHash(m pcommon.Map) Hash {
	h := xxh3.New()
	hashMap(h, m)
	return h.Sum128().Bytes()
}

func hashValue(h *xxh3.Hasher, val pcommon.Value) {
	var buf [8]byte

	switch val.Type() {
	case pcommon.ValueTypeEmpty:
	case pcommon.ValueTypeStr:
		_, _ = h.WriteString(val.Str())
	case pcommon.ValueTypeInt:
		bits := val.Int()
		binary.LittleEndian.PutUint64(buf[:], uint64(bits))
		_, _ = h.Write(buf[:])
	case pcommon.ValueTypeDouble:
		bits := math.Float64bits(val.Double())
		binary.LittleEndian.PutUint64(buf[:], bits)
		_, _ = h.Write(buf[:])
	case pcommon.ValueTypeBool:
		if val.Bool() {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
	case pcommon.ValueTypeBytes:
		// FIXME(tdakkota): AsRaw allocates a new byte slice (opentelemetry moment).
		_, _ = h.Write(val.Bytes().AsRaw())
	case pcommon.ValueTypeSlice:
		slice := val.Slice()

		bits := slice.Len()
		binary.LittleEndian.PutUint64(buf[:], uint64(bits))
		_, _ = h.Write(buf[:])

		for i := 0; i < slice.Len(); i++ {
			hashValue(h, slice.At(i))
		}
	case pcommon.ValueTypeMap:
		hashMap(h, val.Map())
	}
}

func hashMap(h *xxh3.Hasher, m pcommon.Map) {
	type pair struct {
		key   string
		value pcommon.Value
	}
	pairs := make([]pair, 0, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		pairs = append(pairs, pair{
			key:   k,
			value: v,
		})
		return true
	})
	slices.SortFunc(pairs, func(a, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	for _, pair := range pairs {
		hashValue(h, pair.value)
	}
}
