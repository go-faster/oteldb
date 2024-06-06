package chsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInTimeRange(t *testing.T) {
	tests := []struct {
		column string
		start  time.Time
		end    time.Time
		want   string
	}{
		{"timestamp", time.Time{}, time.Time{}, "true"},
		{"timestamp", time.Unix(0, 1), time.Time{}, "timestamp >= toUnixTimestamp64Nano(1)"},
		{"timestamp", time.Time{}, time.Unix(0, 10), "timestamp <= toUnixTimestamp64Nano(10)"},
		{"timestamp", time.Unix(0, 1), time.Unix(0, 10), "timestamp >= toUnixTimestamp64Nano(1) AND timestamp <= toUnixTimestamp64Nano(10)"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got := InTimeRange(tt.column, tt.start, tt.end)

			p := GetPrinter()
			require.NoError(t, p.WriteExpr(got))
			require.Equal(t, tt.want, p.String())
		})
	}
}
