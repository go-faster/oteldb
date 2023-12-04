package logparser

import "time"

// ISO8601Millis time format with millisecond precision.
const ISO8601Millis = "2006-01-02T15:04:05.000Z0700"

// we are not expecting logs from past.
var deductStart = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// deductNanos returns unix nano from arbitrary time integer, deducting resolution by range.
func deductNanos(n int64) (int64, bool) {
	if n > deductStart.UnixNano() {
		return n, true
	}
	if n > deductStart.UnixMicro() {
		return n * 1e3, true
	}
	if n > deductStart.UnixMilli() {
		return n * 1e6, true
	}
	if n > deductStart.Unix() {
		return n * 1e9, true
	}
	return 0, false
}
