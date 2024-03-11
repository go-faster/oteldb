package prompb

import (
	"testing"

	nativeprompb "github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestEnsurePoolReuse(t *testing.T) {
	labels := []nativeprompb.Label{
		{Name: "a", Value: "1"},
		{Name: "b", Value: "2"},
		{Name: "c", Value: "3"},
		{Name: "d", Value: "4"},
	}
	samples := []nativeprompb.Sample{
		{Value: 1, Timestamp: 1},
		{Value: 1, Timestamp: 2},
		{Value: 1, Timestamp: 3},
		{Value: 1, Timestamp: 4},
	}

	data1, err := (&nativeprompb.WriteRequest{
		Timeseries: []nativeprompb.TimeSeries{
			{
				Labels: labels[:2],
			},
			{
				Samples: samples[:4],
			},
		},
	}).Marshal()
	require.NoError(t, err)

	data2, err := (&nativeprompb.WriteRequest{
		Timeseries: []nativeprompb.TimeSeries{
			{
				Samples: samples[:2],
			},
			{
				Labels: labels[:4],
			},
		},
	}).Marshal()
	require.NoError(t, err)

	// Fill target pool.
	target := WriteRequest{
		Timeseries: make([]TimeSeries, 0, 2),
		pools: &pools{
			Labels: &slicepool[Label]{
				pool: make([]Label, 0, 4),
			},
			Samples: &slicepool[Sample]{
				pool: make([]Sample, 0, 4),
			},
		},
	}
	target.pools.init()

	// Ensure that slicepool is properly re-used.
	allocs := testing.AllocsPerRun(10, func() {
		if err := target.Unmarshal(data1); err != nil {
			t.Fatal(err)
		}
		target.Reset()
		if err := target.Unmarshal(data2); err != nil {
			t.Fatal(err)
		}
	})
	require.Zero(t, allocs)
}
