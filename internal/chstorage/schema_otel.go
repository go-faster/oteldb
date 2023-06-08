package chstorage

import (
	"encoding/binary"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

type spanColumns struct {
	traceID       proto.ColUUID
	spanID        proto.ColUInt64
	traceState    proto.ColStr
	parentSpanID  proto.ColUInt64
	name          *proto.ColLowCardinality[string]
	kind          proto.ColEnum8
	start         *proto.ColDateTime64
	end           *proto.ColDateTime64
	spanAttrs     chAttrs
	statusCode    proto.ColInt32
	statusMessage proto.ColStr

	batchID       proto.ColUUID
	resourceAttrs chAttrs

	scopeName    proto.ColStr
	scopeVersion proto.ColStr
	scopeAttrs   chAttrs
}

func newSpanColumns() *spanColumns {
	return &spanColumns{
		name:          new(proto.ColStr).LowCardinality(),
		start:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		end:           new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		spanAttrs:     newChAttrs(),
		resourceAttrs: newChAttrs(),
		scopeAttrs:    newChAttrs(),
	}
}

func (c *spanColumns) Input() proto.Input {
	return proto.Input{
		{Name: "trace_id", Data: c.traceID},
		{Name: "span_id", Data: c.spanID},
		{Name: "trace_state", Data: c.traceState},
		{Name: "parent_span_id", Data: c.parentSpanID},
		{Name: "name", Data: c.name},
		{Name: "kind", Data: proto.Wrap(&c.kind, kindDDL)},
		{Name: "start", Data: c.start},
		{Name: "end", Data: c.end},
		{Name: "attrs_str_keys", Data: c.spanAttrs.StrKeys},
		{Name: "attrs_str_values", Data: c.spanAttrs.StrValues},
		{Name: "attrs_int_keys", Data: c.spanAttrs.IntKeys},
		{Name: "attrs_int_values", Data: c.spanAttrs.IntValues},
		{Name: "attrs_float_keys", Data: c.spanAttrs.FloatKeys},
		{Name: "attrs_float_values", Data: c.spanAttrs.FloatValues},
		{Name: "attrs_bool_keys", Data: c.spanAttrs.BoolKeys},
		{Name: "attrs_bool_values", Data: c.spanAttrs.BoolValues},
		{Name: "attrs_bytes_keys", Data: c.spanAttrs.BytesKeys},
		{Name: "attrs_bytes_values", Data: c.spanAttrs.BytesValues},
		{Name: "status_code", Data: c.statusCode},
		{Name: "status_message", Data: c.statusMessage},

		{Name: "batch_id", Data: c.batchID},
		{Name: "resource_attrs_str_keys", Data: c.resourceAttrs.StrKeys},
		{Name: "resource_attrs_str_values", Data: c.resourceAttrs.StrValues},
		{Name: "resource_attrs_int_keys", Data: c.resourceAttrs.IntKeys},
		{Name: "resource_attrs_int_values", Data: c.resourceAttrs.IntValues},
		{Name: "resource_attrs_float_keys", Data: c.resourceAttrs.FloatKeys},
		{Name: "resource_attrs_float_values", Data: c.resourceAttrs.FloatValues},
		{Name: "resource_attrs_bool_keys", Data: c.resourceAttrs.BoolKeys},
		{Name: "resource_attrs_bool_values", Data: c.resourceAttrs.BoolValues},
		{Name: "resource_attrs_bytes_keys", Data: c.resourceAttrs.BytesKeys},
		{Name: "resource_attrs_bytes_values", Data: c.resourceAttrs.BytesValues},

		{Name: "scope_name", Data: c.scopeName},
		{Name: "scope_version", Data: c.scopeVersion},
		{Name: "scope_attrs_str_keys", Data: c.scopeAttrs.StrKeys},
		{Name: "scope_attrs_str_values", Data: c.scopeAttrs.StrValues},
		{Name: "scope_attrs_int_keys", Data: c.scopeAttrs.IntKeys},
		{Name: "scope_attrs_int_values", Data: c.scopeAttrs.IntValues},
		{Name: "scope_attrs_float_keys", Data: c.scopeAttrs.FloatKeys},
		{Name: "scope_attrs_float_values", Data: c.scopeAttrs.FloatValues},
		{Name: "scope_attrs_bool_keys", Data: c.scopeAttrs.BoolKeys},
		{Name: "scope_attrs_bool_values", Data: c.scopeAttrs.BoolValues},
		{Name: "scope_attrs_bytes_keys", Data: c.scopeAttrs.BytesKeys},
		{Name: "scope_attrs_bytes_values", Data: c.scopeAttrs.BytesValues},
	}
}

func (c *spanColumns) Result() proto.Results {
	return proto.Results{
		{Name: "trace_id", Data: &c.traceID},
		{Name: "span_id", Data: &c.spanID},
		{Name: "trace_state", Data: &c.traceState},
		{Name: "parent_span_id", Data: &c.parentSpanID},
		{Name: "name", Data: c.name},
		{Name: "kind", Data: &c.kind},
		{Name: "start", Data: c.start},
		{Name: "end", Data: c.end},
		{Name: "attrs_str_keys", Data: c.spanAttrs.StrKeys},
		{Name: "attrs_str_values", Data: c.spanAttrs.StrValues},
		{Name: "attrs_int_keys", Data: c.spanAttrs.IntKeys},
		{Name: "attrs_int_values", Data: c.spanAttrs.IntValues},
		{Name: "attrs_float_keys", Data: c.spanAttrs.FloatKeys},
		{Name: "attrs_float_values", Data: c.spanAttrs.FloatValues},
		{Name: "attrs_bool_keys", Data: c.spanAttrs.BoolKeys},
		{Name: "attrs_bool_values", Data: c.spanAttrs.BoolValues},
		{Name: "attrs_bytes_keys", Data: c.spanAttrs.BytesKeys},
		{Name: "attrs_bytes_values", Data: c.spanAttrs.BytesValues},
		{Name: "status_code", Data: &c.statusCode},
		{Name: "status_message", Data: &c.statusMessage},

		{Name: "batch_id", Data: &c.batchID},
		{Name: "resource_attrs_str_keys", Data: c.resourceAttrs.StrKeys},
		{Name: "resource_attrs_str_values", Data: c.resourceAttrs.StrValues},
		{Name: "resource_attrs_int_keys", Data: c.resourceAttrs.IntKeys},
		{Name: "resource_attrs_int_values", Data: c.resourceAttrs.IntValues},
		{Name: "resource_attrs_float_keys", Data: c.resourceAttrs.FloatKeys},
		{Name: "resource_attrs_float_values", Data: c.resourceAttrs.FloatValues},
		{Name: "resource_attrs_bool_keys", Data: c.resourceAttrs.BoolKeys},
		{Name: "resource_attrs_bool_values", Data: c.resourceAttrs.BoolValues},
		{Name: "resource_attrs_bytes_keys", Data: c.resourceAttrs.BytesKeys},
		{Name: "resource_attrs_bytes_values", Data: c.resourceAttrs.BytesValues},

		{Name: "scope_name", Data: &c.scopeName},
		{Name: "scope_version", Data: &c.scopeVersion},
		{Name: "scope_attrs_str_keys", Data: c.scopeAttrs.StrKeys},
		{Name: "scope_attrs_str_values", Data: c.scopeAttrs.StrValues},
		{Name: "scope_attrs_int_keys", Data: c.scopeAttrs.IntKeys},
		{Name: "scope_attrs_int_values", Data: c.scopeAttrs.IntValues},
		{Name: "scope_attrs_float_keys", Data: c.scopeAttrs.FloatKeys},
		{Name: "scope_attrs_float_values", Data: c.scopeAttrs.FloatValues},
		{Name: "scope_attrs_bool_keys", Data: c.scopeAttrs.BoolKeys},
		{Name: "scope_attrs_bool_values", Data: c.scopeAttrs.BoolValues},
		{Name: "scope_attrs_bytes_keys", Data: c.scopeAttrs.BytesKeys},
		{Name: "scope_attrs_bytes_values", Data: c.scopeAttrs.BytesValues},
	}
}

func (c *spanColumns) Append(s tracestorage.Span) {
	getSpanID := func(s tracestorage.SpanID) uint64 {
		return binary.LittleEndian.Uint64(s[:])
	}

	c.traceID.Append(uuid.UUID(s.TraceID))
	c.spanID.Append(getSpanID(s.SpanID))
	c.traceState.Append(s.TraceState)
	c.parentSpanID.Append(getSpanID(s.ParentSpanID))
	c.name.Append(s.Name)
	c.kind.Append(proto.Enum8(s.Kind))
	c.start.Append(time.Unix(0, int64(s.Start)))
	c.end.Append(time.Unix(0, int64(s.End)))
	c.spanAttrs.Append(s.Attrs)
	c.statusCode.Append(s.StatusCode)
	c.statusMessage.Append(s.StatusMessage)
	// FIXME(tdakkota): use UUID in Span.
	c.batchID.Append(uuid.MustParse(s.BatchID))
	c.resourceAttrs.Append(s.ResourceAttrs)
	c.scopeName.Append(s.ScopeName)
	c.scopeVersion.Append(s.ScopeVersion)
	c.scopeAttrs.Append(s.ScopeAttrs)
}

func (c *spanColumns) CollectAppend(spans []tracestorage.Span) []tracestorage.Span {
	getSpanID := func(v uint64) (r tracestorage.SpanID) {
		binary.LittleEndian.PutUint64(r[:], v)
		return
	}

	for i := 0; i < c.traceID.Rows(); i++ {
		spans = append(spans, tracestorage.Span{
			TraceID:       tracestorage.TraceID(c.traceID.Row(i)),
			SpanID:        getSpanID(c.spanID.Row(i)),
			TraceState:    c.traceState.Row(i),
			ParentSpanID:  getSpanID(c.parentSpanID.Row(i)),
			Name:          c.name.Row(i),
			Kind:          int32(c.kind.Row(i)),
			Start:         uint64(c.start.Row(i).UnixNano()),
			End:           uint64(c.end.Row(i).UnixNano()),
			Attrs:         c.spanAttrs.Row(i),
			StatusCode:    c.statusCode.Row(i),
			StatusMessage: c.statusMessage.Row(i),
			BatchID:       c.batchID.Row(i).String(),
			ResourceAttrs: c.resourceAttrs.Row(i),
			ScopeName:     c.scopeName.Row(i),
			ScopeVersion:  c.scopeVersion.Row(i),
			ScopeAttrs:    c.scopeAttrs.Row(i),
		})
	}

	return spans
}

type chAttrs struct {
	StrKeys     *proto.ColArr[string]
	StrValues   *proto.ColArr[string]
	IntKeys     *proto.ColArr[string]
	IntValues   *proto.ColArr[int64]
	FloatKeys   *proto.ColArr[string]
	FloatValues *proto.ColArr[float64]
	BoolKeys    *proto.ColArr[string]
	BoolValues  *proto.ColArr[bool]
	BytesKeys   *proto.ColArr[string]
	BytesValues *proto.ColArr[string]
}

func newChAttrs() chAttrs {
	return chAttrs{
		StrKeys:     new(proto.ColStr).LowCardinality().Array(),
		StrValues:   new(proto.ColStr).Array(),
		IntKeys:     new(proto.ColStr).LowCardinality().Array(),
		IntValues:   new(proto.ColInt64).Array(),
		FloatKeys:   new(proto.ColStr).LowCardinality().Array(),
		FloatValues: new(proto.ColFloat64).Array(),
		BoolKeys:    new(proto.ColStr).LowCardinality().Array(),
		BoolValues:  new(proto.ColBool).Array(),
		BytesKeys:   new(proto.ColStr).LowCardinality().Array(),
		BytesValues: new(proto.ColStr).Array(),
	}
}

func (c *chAttrs) Append(attrs tracestorage.Attrs) {
	var (
		strKeys     []string
		strValues   []string
		intKeys     []string
		intValues   []int64
		floatKeys   []string
		floatValues []float64
		boolKeys    []string
		boolValues  []bool
		bytesKeys   []string
		bytesValues []string
	)

	attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeStr:
			strKeys = append(strKeys, k)
			strValues = append(strValues, v.Str())
		case pcommon.ValueTypeBool:
			boolKeys = append(boolKeys, k)
			boolValues = append(boolValues, v.Bool())
		case pcommon.ValueTypeInt:
			intKeys = append(intKeys, k)
			intValues = append(intValues, v.Int())
		case pcommon.ValueTypeDouble:
			floatKeys = append(floatKeys, k)
			floatValues = append(floatValues, v.Double())
		case pcommon.ValueTypeBytes:
			bytesKeys = append(bytesKeys, k)
			bytesValues = append(bytesValues, string(v.Bytes().AsRaw()))
		}
		return true
	})

	c.StrKeys.Append(strKeys)
	c.StrValues.Append(strValues)
	c.IntKeys.Append(intKeys)
	c.IntValues.Append(intValues)
	c.FloatKeys.Append(floatKeys)
	c.FloatValues.Append(floatValues)
	c.BoolKeys.Append(boolKeys)
	c.BoolValues.Append(boolValues)
	c.BytesKeys.Append(bytesKeys)
	c.BytesValues.Append(bytesValues)
}

func (c *chAttrs) Row(row int) tracestorage.Attrs {
	m := pcommon.NewMap()

	var (
		strKeys     = c.StrKeys.Row(row)
		strValues   = c.StrValues.Row(row)
		intKeys     = c.IntKeys.Row(row)
		intValues   = c.IntValues.Row(row)
		floatKeys   = c.FloatKeys.Row(row)
		floatValues = c.FloatValues.Row(row)
		boolKeys    = c.BoolKeys.Row(row)
		boolValues  = c.BoolValues.Row(row)
		bytesKeys   = c.BytesKeys.Row(row)
		bytesValues = c.BytesValues.Row(row)
	)

	for i, key := range strKeys {
		m.PutStr(key, strValues[i])
	}
	for i, key := range intKeys {
		m.PutInt(key, intValues[i])
	}
	for i, key := range floatKeys {
		m.PutDouble(key, floatValues[i])
	}
	for i, key := range boolKeys {
		m.PutBool(key, boolValues[i])
	}
	for i, key := range bytesKeys {
		data := m.PutEmptyBytes(key)
		data.FromRaw([]byte(bytesValues[i]))
	}

	return tracestorage.Attrs(m)
}
