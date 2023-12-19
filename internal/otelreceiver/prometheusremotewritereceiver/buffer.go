package prometheusremotewritereceiver

import (
	"bytes"
	"sync"
)

var bufPool = sync.Pool{
	New: func() any {
		var buf bytes.Buffer
		buf.Grow(32 * 1024)
		return &buf
	},
}

func getBuf() *bytes.Buffer {
	buf := bufPool.Get()
	return buf.(*bytes.Buffer)
}

func putBuf(buf *bytes.Buffer) {
	buf.Reset()
	bufPool.Put(buf)
}

type closerReader struct {
	data []byte
	bytes.Reader
}

func (c *closerReader) Close() error {
	return nil
}
