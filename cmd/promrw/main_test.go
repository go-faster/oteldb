package main

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestProtobufReadWrite(t *testing.T) {
	req := &prompb.WriteRequest{
		Metadata: []prompb.MetricMetadata{
			{
				Type: prompb.MetricMetadata_GAUGE,
				Help: "Help",
			},
		},
	}
	out := new(bytes.Buffer)
	w := gob.NewEncoder(out)
	for i := 0; i < 10; i++ {
		data, err := req.Marshal()
		require.NoError(t, err)
		require.NoError(t, w.Encode(data))
	}

	r := gob.NewDecoder(out)
	for i := 0; i < 10; i++ {
		var data []byte
		require.NoError(t, r.Decode(&data))
		var got prompb.WriteRequest
		require.NoError(t, got.Unmarshal(data))
		require.Equal(t, req.Metadata, got.Metadata)
	}
}
