package prometheusremotewritereceiver

import (
	"bytes"

	"github.com/go-faster/oteldb/internal/prompb"
	"github.com/go-faster/oteldb/internal/xsync"
)

var writeRequestPool = xsync.NewPool(func() *prompb.WriteRequest {
	return &prompb.WriteRequest{}
})

type closerReader struct {
	data []byte
	bytes.Reader
}

func (c *closerReader) Close() error {
	return nil
}
