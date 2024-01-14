package otelbench

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/go-faster/errors"
)

const protocolLengthBytes = 4

// Reader reads data from an io.Reader and decodes it as "rwq" protocol.
type Reader struct {
	err    error
	reader io.Reader
	data   []byte
}

func (r *Reader) Decode() bool {
	if r.err != nil {
		return false
	}
	// Read length.
	var length [protocolLengthBytes]byte
	_, r.err = io.ReadFull(r.reader, length[:])
	if r.err != nil {
		return false
	}
	// Read data.
	l := binary.BigEndian.Uint32(length[:])
	if cap(r.data) < int(l) {
		r.data = make([]byte, l)
	} else {
		r.data = r.data[:l]
	}
	_, r.err = io.ReadFull(r.reader, r.data)
	return r.err == nil
}

func (r *Reader) Data() []byte { return r.data }

func (r *Reader) Err() error {
	if r.err == io.EOF {
		return nil
	}
	return r.err
}

func NewReader(r io.Reader) *Reader {
	return &Reader{reader: r}
}

// Writer writes data to an io.Writer as "rwq" protocol.
type Writer struct {
	writer io.Writer
	mux    sync.Mutex
}

func (w *Writer) Encode(data []byte) error {
	w.mux.Lock()
	defer w.mux.Unlock()
	var length [protocolLengthBytes]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(data)))
	if _, err := w.writer.Write(length[:]); err != nil {
		return errors.Wrap(err, "write length")
	}
	if _, err := w.writer.Write(data); err != nil {
		return errors.Wrap(err, "write data")
	}
	return nil
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}
