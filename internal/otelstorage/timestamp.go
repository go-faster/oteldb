package otelstorage

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Timestamp is a time specified as UNIX Epoch time in nanoseconds since
// 1970-01-01 00:00:00 +0000 UTC.
type Timestamp = pcommon.Timestamp

// NewTimestampFromTime creates new Timestamp from time.Time.
func NewTimestampFromTime(t time.Time) Timestamp {
	return pcommon.NewTimestampFromTime(t)
}
