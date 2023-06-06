package tracestorage

import (
	"encoding/binary"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.ytsaurus.tech/yt/go/yson"

	"github.com/go-faster/errors"
)

// TraceID is OpenTelemetry trace ID.
type TraceID [16]byte

var (
	_ yson.StreamMarshaler   = TraceID{}
	_ yson.StreamUnmarshaler = (*TraceID)(nil)
)

// Hex returns a hex representation of TraceID.
func (id TraceID) Hex() string {
	const hextable = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(len(id) * 2)
	for _, c := range id {
		sb.WriteByte(hextable[c>>4])
		sb.WriteByte(hextable[c&0x0f])
	}
	return sb.String()
}

// MarshalYSON implemenets yson.StreamMarshaler.
func (id TraceID) MarshalYSON(w *yson.Writer) error {
	w.Bytes(id[:])
	return nil
}

// UnmarshalYSON implemenets yson.StreamUnmarshaler.
func (id *TraceID) UnmarshalYSON(r *yson.Reader) error {
	if err := consumeYsonLiteral(r, yson.TypeString); err != nil {
		return err
	}
	data := r.Bytes()

	const expectedLen = len(TraceID{})
	if got := len(data); expectedLen != got {
		return errors.Errorf("expected %d bytes, got %d", expectedLen, got)
	}

	copy(id[:], data)
	return nil
}

// SpanID is OpenTelemetry span ID.
type SpanID [8]byte

var (
	_ yson.StreamMarshaler   = SpanID{}
	_ yson.StreamUnmarshaler = (*SpanID)(nil)
)

// IsEmpty returns true if span ID is empty.
func (id SpanID) IsEmpty() bool {
	return pcommon.SpanID(id).IsEmpty()
}

// Hex returns a hex representation of SpanID.
func (id SpanID) Hex() string {
	const hextable = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(len(id) * 2)
	for _, c := range id {
		sb.WriteByte(hextable[c>>4])
		sb.WriteByte(hextable[c&0x0f])
	}
	return sb.String()
}

// MarshalYSON implemenets yson.StreamMarshaler.
func (id SpanID) MarshalYSON(w *yson.Writer) error {
	if id.IsEmpty() {
		w.Entity()
	} else {
		w.Uint64(binary.LittleEndian.Uint64(id[:]))
	}
	return nil
}

// UnmarshalYSON implemenets yson.StreamUnmarshaler.
func (id *SpanID) UnmarshalYSON(r *yson.Reader) error {
	if err := ysonNext(r, yson.EventLiteral, false); err != nil {
		return err
	}
	switch got := r.Type(); got {
	case yson.TypeEntity:
		// It's null
		*id = SpanID{}
		return nil
	case yson.TypeUint64:
		v := r.Uint64()
		binary.LittleEndian.PutUint64(id[:], v)
		return nil
	default:
		return errors.Errorf("expected %s or %s, got %s", yson.TypeEntity, yson.TypeUint64, got)
	}
}
