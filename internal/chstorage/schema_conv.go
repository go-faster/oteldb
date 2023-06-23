package chstorage

import (
	"encoding/binary"
	"time"

	"golang.org/x/exp/constraints"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

func getSpanID(v uint64) (r tracestorage.SpanID) {
	binary.LittleEndian.PutUint64(r[:], v)
	return
}

func putSpanID(s tracestorage.SpanID) uint64 {
	return binary.LittleEndian.Uint64(s[:])
}

func getTimestamp(t time.Time) tracestorage.Timestamp {
	return uint64(t.UnixNano())
}

func putTimestamp(t tracestorage.Timestamp) time.Time {
	return time.Unix(0, int64(t))
}

func minimum[T constraints.Integer](vals ...T) (min T) {
	if len(vals) < 1 {
		return min
	}
	min = vals[0]
	for _, val := range vals[1:] {
		if val < min {
			min = val
		}
	}
	return min
}
