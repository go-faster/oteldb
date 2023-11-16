package metricsharding

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/metricstorage"
)

type row struct {
	ts  int64
	val float64
}

func TestPointIterator(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		piter := newPointIterator([]metricstorage.Point{})
		piter.Next()
	})
	t.Run("Iter", func(t *testing.T) {
		piter := newPointIterator([]metricstorage.Point{
			{Timestamp: pcommon.Timestamp(1 * time.Second), Point: 1},
			{Timestamp: pcommon.Timestamp(2 * time.Second), Point: 2},
			{Timestamp: pcommon.Timestamp(3 * time.Second), Point: 3},
		})

		var result []row
		for piter.Next() != chunkenc.ValNone {
			ts, val := piter.At()
			require.Equal(t, ts, piter.AtT())

			result = append(result, row{
				ts:  ts,
				val: val,
			})
		}
		// Expect timestamp in milliseconds.
		require.Equal(t, []row{
			{1_000, 1},
			{2_000, 2},
			{3_000, 3},
		}, result)
	})
}

func TestPointIteratorSeek(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		piter := newPointIterator([]metricstorage.Point{})
		piter.Seek(10)
	})
	t.Run("Seek", func(t *testing.T) {
		tests := []struct {
			seek   time.Duration
			result chunkenc.ValueType
			rows   []row
		}{
			{
				9 * time.Second,
				chunkenc.ValFloat,
				[]row{
					{10_000, 1},
					{20_000, 2},
					{30_000, 3},
				},
			},
			{
				15 * time.Second,
				chunkenc.ValFloat,
				[]row{
					{20_000, 2},
					{30_000, 3},
				},
			},
			{
				26 * time.Second,
				chunkenc.ValFloat,
				[]row{
					{30_000, 3},
				},
			},
			{
				40 * time.Second,
				chunkenc.ValNone,
				nil,
			},
		}
		for i, tt := range tests {
			tt := tt
			t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
				piter := newPointIterator([]metricstorage.Point{
					{Timestamp: pcommon.Timestamp(10 * time.Second), Point: 1},
					{Timestamp: pcommon.Timestamp(20 * time.Second), Point: 2},
					{Timestamp: pcommon.Timestamp(30 * time.Second), Point: 3},
				})
				got := piter.Seek(tt.seek.Milliseconds())
				require.Equal(t, tt.result, got)

				var gotRows []row
				for piter.Next() != chunkenc.ValNone {
					ts, val := piter.At()
					require.Equal(t, ts, piter.AtT())

					gotRows = append(gotRows, row{
						ts:  ts,
						val: val,
					})
				}
				require.Equal(t, tt.rows, gotRows)
			})
		}
	})
}
