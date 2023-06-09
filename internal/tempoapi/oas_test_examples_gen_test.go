// Code generated by ogen, DO NOT EDIT.

package tempoapi

import (
	"github.com/go-faster/jx"

	std "encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnyValue_EncodeDecode(t *testing.T) {
	var typ AnyValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 AnyValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestArrayValue_EncodeDecode(t *testing.T) {
	var typ ArrayValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 ArrayValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestAttributes_EncodeDecode(t *testing.T) {
	var typ Attributes
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 Attributes
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestBoolValue_EncodeDecode(t *testing.T) {
	var typ BoolValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 BoolValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestBytesValue_EncodeDecode(t *testing.T) {
	var typ BytesValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 BytesValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestDoubleValue_EncodeDecode(t *testing.T) {
	var typ DoubleValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 DoubleValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestError_EncodeDecode(t *testing.T) {
	var typ Error
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 Error
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestIntValue_EncodeDecode(t *testing.T) {
	var typ IntValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 IntValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestKeyValue_EncodeDecode(t *testing.T) {
	var typ KeyValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 KeyValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestKvlistValue_EncodeDecode(t *testing.T) {
	var typ KvlistValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 KvlistValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestStringValue_EncodeDecode(t *testing.T) {
	var typ StringValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 StringValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestTagNames_EncodeDecode(t *testing.T) {
	var typ TagNames
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 TagNames
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestTagValue_EncodeDecode(t *testing.T) {
	var typ TagValue
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 TagValue
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestTagValues_EncodeDecode(t *testing.T) {
	var typ TagValues
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 TagValues
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestTagValuesV2_EncodeDecode(t *testing.T) {
	var typ TagValuesV2
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 TagValuesV2
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestTempoSpan_EncodeDecode(t *testing.T) {
	var typ TempoSpan
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 TempoSpan
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestTempoSpanSet_EncodeDecode(t *testing.T) {
	var typ TempoSpanSet
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 TempoSpanSet
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestTraceSearchMetadata_EncodeDecode(t *testing.T) {
	var typ TraceSearchMetadata
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 TraceSearchMetadata
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
func TestTraces_EncodeDecode(t *testing.T) {
	var typ Traces
	typ.SetFake()

	e := jx.Encoder{}
	typ.Encode(&e)
	data := e.Bytes()
	require.True(t, std.Valid(data), "Encoded: %s", data)

	var typ2 Traces
	require.NoError(t, typ2.Decode(jx.DecodeBytes(data)))
}
