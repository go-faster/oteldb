package otelstorage

import (
	"encoding/binary"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.ytsaurus.tech/yt/go/yson"

	"github.com/go-faster/errors"
	"github.com/google/uuid"
)

// TraceID is OpenTelemetry trace ID.
type TraceID [16]byte

// ParseTraceID parses trace ID from given string.
//
// Deals with missing leading zeroes.
func ParseTraceID(input string) (_ TraceID, err error) {
	var id uuid.UUID

	if len(input) >= 32 {
		// Normal UUID.
		id, err = uuid.Parse(input)
	} else {
		// UUID without leading zeroes.
		var hex [32]byte
		// FIXME(tdakkota): probably, it's faster to define some variable with zeroes
		// 	and just copy it.
		for i := range hex {
			hex[i] = '0'
		}
		// Copy input to the end of hex array.
		//
		//  00000000000000000000000000000000 <- hex, len = 32
		//    b78e08df6f20dc3ad29d3915beab75 <- input, len = 30
		//  00b78e08df6f20dc3ad29d3915beab75 <- copy(hex[32-30:], input)
		//
		copy(hex[len(hex)-len(input):], input)
		id, err = uuid.ParseBytes(hex[:])
	}
	return TraceID(id), err
}

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
	if id == nil {
		return errors.Errorf("can't unmarshal to %#v", id)
	}
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

// SpanIDFromUint64 creates new SpanID from uint64.
func SpanIDFromUint64(v uint64) (r SpanID) {
	binary.LittleEndian.PutUint64(r[:], v)
	return r
}

// IsEmpty returns true if span ID is empty.
func (id SpanID) IsEmpty() bool {
	return pcommon.SpanID(id).IsEmpty()
}

// AsUint64 returns span ID as LittleEndian uint64.
func (id SpanID) AsUint64() uint64 {
	return binary.LittleEndian.Uint64(id[:])
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
		w.Uint64(id.AsUint64())
	}
	return nil
}

// UnmarshalYSON implemenets yson.StreamUnmarshaler.
func (id *SpanID) UnmarshalYSON(r *yson.Reader) error {
	if id == nil {
		return errors.Errorf("can't unmarshal to %#v", id)
	}
	if err := ysonNext(r, yson.EventLiteral, false); err != nil {
		return err
	}
	switch got := r.Type(); got {
	case yson.TypeEntity:
		// It's null
		*id = SpanID{}
		return nil
	case yson.TypeUint64:
		*id = SpanIDFromUint64(r.Uint64())
		return nil
	default:
		return errors.Errorf("expected %s or %s, got %s", yson.TypeEntity, yson.TypeUint64, got)
	}
}
