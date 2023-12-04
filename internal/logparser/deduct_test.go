package logparser

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeductNanos(t *testing.T) {
	var (
		start = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		end   = time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)
	)
	truncate := func(nanos int64, zeroes int) int64 {
		d := int64(math.Pow10(zeroes))
		return nanos - nanos%d
	}
	assert := func(a, b int64, msgAndArgs ...interface{}) {
		t.Helper()
		v, ok := deductNanos(a)
		require.True(t, ok, msgAndArgs...)
		require.Equal(t, b, v, msgAndArgs...)
	}
	for v := start; v.Before(end); v = v.Add(time.Second*44 + time.Nanosecond*1337123 + time.Hour*6) {
		expected := v.UnixNano()
		assert(v.Unix(), truncate(expected, 9), "v=%v", v)
		assert(v.UnixMilli(), truncate(expected, 6), "v=%v", v)
		assert(v.UnixMicro(), truncate(expected, 3), "v=%v", v)
		assert(v.UnixNano(), expected, "v=%v", v)
	}
}
