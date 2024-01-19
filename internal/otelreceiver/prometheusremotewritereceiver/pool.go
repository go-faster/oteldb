package prometheusremotewritereceiver

import (
	"bytes"
	"sync"

	"github.com/go-faster/oteldb/internal/prompb"
)

var writeRequestPool sync.Pool

func putWriteRequest(wr *prompb.WriteRequest) {
	wr.Reset()
	writeRequestPool.Put(wr)
}

func getWriteRequest() *prompb.WriteRequest {
	v := writeRequestPool.Get()
	if v == nil {
		return &prompb.WriteRequest{}
	}
	return v.(*prompb.WriteRequest)
}

type closerReader struct {
	data []byte
	bytes.Reader
}

func (c *closerReader) Close() error {
	return nil
}
