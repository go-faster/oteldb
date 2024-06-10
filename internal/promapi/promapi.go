// Package promapi contains generated code for OpenAPI specification.
package promapi

import (
	"math"
	"time"
)

// https://github.com/prometheus/prometheus/blob/e9b94515caa4c0d7a0e31f722a1534948ebad838/web/api/v1/api.go#L783-L794
var (
	// MinTime is the default timestamp used for the begin of optional time ranges.
	// Exposed to let downstream projects to reference it.
	MinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()

	// MaxTime is the default timestamp used for the end of optional time ranges.
	// Exposed to let downstream projects to reference it.
	MaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = MinTime.Format(time.RFC3339Nano)
	maxTimeFormatted = MaxTime.Format(time.RFC3339Nano)
)
