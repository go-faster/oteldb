package otelstorage

import (
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"math"
	"slices"

	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Hash is an attribute hash.
type Hash [16]byte

func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

// AttrHash computes attributes hash.
func AttrHash(m pcommon.Map) Hash {
	h := xxh3.New()
	hashMap(h, m)
	return h.Sum128().Bytes()
}

// StrHash computes string hash.
func StrHash(s string) Hash {
	h := xxh3.New()
	_, _ = h.WriteString(s)
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
