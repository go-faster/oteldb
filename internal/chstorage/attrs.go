package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

type chArrAttrCollector struct {
	StrKeys     [][]string
	StrValues   [][]string
	IntKeys     [][]string
	IntValues   [][]int64
	FloatKeys   [][]string
	FloatValues [][]float64
	BoolKeys    [][]string
	BoolValues  [][]bool
	BytesKeys   [][]string
	BytesValues [][]string
}

func (c *chArrAttrCollector) Append(attrs otelstorage.Attrs) {
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

	c.StrKeys = append(c.StrKeys, strKeys)
	c.StrValues = append(c.StrValues, strValues)
	c.IntKeys = append(c.IntKeys, intKeys)
	c.IntValues = append(c.IntValues, intValues)
	c.FloatKeys = append(c.FloatKeys, floatKeys)
	c.FloatValues = append(c.FloatValues, floatValues)
	c.BoolKeys = append(c.BoolKeys, boolKeys)
	c.BoolValues = append(c.BoolValues, boolValues)
	c.BytesKeys = append(c.BytesKeys, bytesKeys)
	c.BytesValues = append(c.BytesValues, bytesValues)
}

func (c *chArrAttrCollector) AddRow(to *chArrAttrs) {
	to.StrKeys.Append(c.StrKeys)
	to.StrValues.Append(c.StrValues)
	to.IntKeys.Append(c.IntKeys)
	to.IntValues.Append(c.IntValues)
	to.FloatKeys.Append(c.FloatKeys)
	to.FloatValues.Append(c.FloatValues)
	to.BoolKeys.Append(c.BoolKeys)
	to.BoolValues.Append(c.BoolValues)
	to.BytesKeys.Append(c.BytesKeys)
	to.BytesValues.Append(c.BytesValues)
}

type chArrAttrs struct {
	StrKeys     *proto.ColArr[[]string]
	StrValues   *proto.ColArr[[]string]
	IntKeys     *proto.ColArr[[]string]
	IntValues   *proto.ColArr[[]int64]
	FloatKeys   *proto.ColArr[[]string]
	FloatValues *proto.ColArr[[]float64]
	BoolKeys    *proto.ColArr[[]string]
	BoolValues  *proto.ColArr[[]bool]
	BytesKeys   *proto.ColArr[[]string]
	BytesValues *proto.ColArr[[]string]
}

func arrayOfArray[T any, P proto.Arrayable[T]](e P) *proto.ColArr[[]T] {
	return proto.NewArray[[]T](e.Array())
}

func newChArrAttrs() chArrAttrs {
	return chArrAttrs{
		StrKeys:     arrayOfArray[string](new(proto.ColStr).LowCardinality()),
		StrValues:   arrayOfArray[string](new(proto.ColStr)),
		IntKeys:     arrayOfArray[string](new(proto.ColStr).LowCardinality()),
		IntValues:   arrayOfArray[int64](new(proto.ColInt64)),
		FloatKeys:   arrayOfArray[string](new(proto.ColStr).LowCardinality()),
		FloatValues: arrayOfArray[float64](new(proto.ColFloat64)),
		BoolKeys:    arrayOfArray[string](new(proto.ColStr).LowCardinality()),
		BoolValues:  arrayOfArray[bool](new(proto.ColBool)),
		BytesKeys:   arrayOfArray[string](new(proto.ColStr).LowCardinality()),
		BytesValues: arrayOfArray[string](new(proto.ColStr)),
	}
}

func (c *chArrAttrs) Row(row int) (result []otelstorage.Attrs) {
	var (
		rowStrKeys     = c.StrKeys.Row(row)
		rowStrValues   = c.StrValues.Row(row)
		rowIntKeys     = c.IntKeys.Row(row)
		rowIntValues   = c.IntValues.Row(row)
		rowFloatKeys   = c.FloatKeys.Row(row)
		rowFloatValues = c.FloatValues.Row(row)
		rowBoolKeys    = c.BoolKeys.Row(row)
		rowBoolValues  = c.BoolValues.Row(row)
		rowBytesKeys   = c.BytesKeys.Row(row)
		rowBytesValues = c.BytesValues.Row(row)
	)
	for i := range rowStrKeys {
		var (
			strKeys     = rowStrKeys[i]
			strValues   = rowStrValues[i]
			intKeys     = rowIntKeys[i]
			intValues   = rowIntValues[i]
			floatKeys   = rowFloatKeys[i]
			floatValues = rowFloatValues[i]
			boolKeys    = rowBoolKeys[i]
			boolValues  = rowBoolValues[i]
			bytesKeys   = rowBytesKeys[i]
			bytesValues = rowBytesValues[i]
		)

		m := pcommon.NewMap()
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
		result = append(result, otelstorage.Attrs(m))
	}
	return result
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

func (c *chAttrs) Append(attrs otelstorage.Attrs) {
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

func (c *chAttrs) Row(row int) otelstorage.Attrs {
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

	return otelstorage.Attrs(m)
}
