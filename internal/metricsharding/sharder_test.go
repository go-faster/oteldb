package metricsharding

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_timeBlocksForRange(t *testing.T) {
	someStart := time.Date(2023, time.September, 2, 23, 0, 0, 0, time.UTC)
	someEnd := time.Date(2023, time.September, 4, 23, 0, 0, 0, time.UTC)

	tests := []struct {
		dirs   []string
		start  time.Time
		end    time.Time
		expect []string
	}{
		{
			nil,
			someStart,
			someEnd,
			nil,
		},
		// No matching.
		{
			[]string{
				// Too far in the future -> drop.
				`2077-01-01_00-00-00`,
			},
			someStart,
			someEnd,
			nil,
		},
		{
			[]string{
				// Too far in the future -> drop.
				`2076-01-01_00-00-00`,
				`2077-01-01_00-00-00`,
			},
			someStart,
			someEnd,
			nil,
		},
		// Match
		{
			[]string{
				// Starts before given start and still continues up to now -> keep.
				`2023-09-01_00-00-00`,
			},
			someStart,
			someEnd,
			[]string{
				`2023-09-01_00-00-00`,
			},
		},
		{
			[]string{
				// Starts before given start and still continues up to now -> keep.
				`2023-09-01_00-00-00`,
				// Starts after given end -> drop.
				`2023-09-05_00-00-00`,
			},
			someStart,
			someEnd,
			[]string{
				`2023-09-01_00-00-00`,
			},
		},
		{
			[]string{
				// Starts before given start and ends before given end  -> keep.
				`2023-09-01_00-00-00`,
				// Starts after given start and still continues up to now -> keep.
				`2023-09-03_00-00-00`,
			},
			someStart,
			someEnd,
			[]string{
				`2023-09-01_00-00-00`,
				`2023-09-03_00-00-00`,
			},
		},
		{
			[]string{
				// Starts before given start and ends before given end -> keep.
				`2023-09-01_00-00-00`,
				// Starts after given start and ends before given end -> keep.
				`2023-09-03_00-00-00`,
				// Starts after given end -> drop.
				`2023-09-06_00-00-00`,
			},
			someStart,
			someEnd,
			[]string{
				`2023-09-01_00-00-00`,
				`2023-09-03_00-00-00`,
			},
		},
		{
			[]string{
				// Ends before start -> drop.
				`2001-01-01_00-00-00`,
				`2023-09-01_00-00-00`,

				// Within range -> keep.
				`2023-09-02_00-00-00`,
				`2023-09-03_00-00-00`,
				`2023-09-04_00-00-00`,

				// Too far in the future -> drop.
				`2023-09-05_00-00-00`,
				`2077-01-01_00-00-00`,
			},
			someStart,
			someEnd,
			[]string{
				`2023-09-02_00-00-00`,
				`2023-09-03_00-00-00`,
				`2023-09-04_00-00-00`,
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			timeShards := make([]timeBlock, len(tt.dirs))
			for i, dir := range tt.dirs {
				at, err := time.Parse(timeBlockLayout, dir)
				require.NoError(t, err)
				timeShards[i] = timeBlock{
					start: at,
					dir:   dir,
				}
			}

			var got []string
			for _, shard := range timeBlocksForRange(timeShards, tt.start, tt.end) {
				got = append(got, shard.dir)
			}
			require.Equal(t, tt.expect, got)
		})
	}
}
