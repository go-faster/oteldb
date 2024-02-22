package prompb_test

import (
	"fmt"
	"testing"

	nativeprompb "github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/prompb"
)

func equalWriteRequest(t *testing.T, a nativeprompb.WriteRequest, b prompb.WriteRequest) {
	t.Helper()

	equalSlices(t, a.Timeseries, b.Timeseries, equalTimeseries)
}

func equalTimeseries(t *testing.T, a nativeprompb.TimeSeries, b prompb.TimeSeries) {
	t.Helper()

	equalSlices(t, a.Labels, b.Labels, equalLabels)
	equalSlices(t, a.Samples, b.Samples, equalSamples)
}

func equalLabels(t *testing.T, a nativeprompb.Label, b prompb.Label) {
	t.Helper()

	assert.Equal(t, a.Name, string(b.Name))
	assert.Equal(t, a.Value, string(b.Value))
}

func equalSamples(t *testing.T, a nativeprompb.Sample, b prompb.Sample) {
	t.Helper()

	assert.Equal(t, a.Value, b.Value)
	assert.Equal(t, a.Timestamp, b.Timestamp)
}

func equalSlices[A, B any](t *testing.T, a []A, b []B, cmp func(*testing.T, A, B)) {
	t.Helper()

	assert.Len(t, b, len(a))
	for i, aElem := range a {
		cmp(t, aElem, b[i])
	}
}

var writeRequestTests = []nativeprompb.WriteRequest{
	{},
	{
		Timeseries: []nativeprompb.TimeSeries{
			{
				Labels: []nativeprompb.Label{
					{Name: "foo", Value: "bar"},
					{Name: "far", Value: "boo"},
				},
				Samples: []nativeprompb.Sample{
					{Value: 1.3, Timestamp: 10},
					{Value: 2.3, Timestamp: 11},
				},
			},
		},
	},
}

func TestWriteRequest(t *testing.T) {
	for i, req := range writeRequestTests {
		native := req
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data, err := native.Marshal()
			require.NoError(t, err)

			var target prompb.WriteRequest
			require.NoError(t, target.Unmarshal(data))

			equalWriteRequest(t, native, target)
		})
	}
}

func FuzzWriteRequest(f *testing.F) {
	for _, req := range writeRequestTests {
		data, err := req.Marshal()
		require.NoError(f, err)

		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var native nativeprompb.WriteRequest
		if err := native.Unmarshal(data); err != nil {
			t.Skipf("Invalid input: %+v", err)
			return
		}

		var target prompb.WriteRequest
		require.NoError(t, target.Unmarshal(data))
	})
}
