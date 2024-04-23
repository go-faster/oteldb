package oteldbproto

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// MetricsOpenTelemetry is an adapter to encode/decode metrics as OpenTelemetry protobuf.
type MetricsOpenTelemetry struct {
	Metrics
}

// Unmarshal unmarshals [MetricsOpenTelemetry] from MetricsData.
func (s *MetricsOpenTelemetry) Unmarshal(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1: // resource_metrics
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read resource_metrics (field %d)", fc.FieldNum)
			}
			if err := s.unmarshalResourceMetrics(value); err != nil {
				return errors.Wrap(err, "unmarshal resource_metrics")
			}
		}
	}
	return nil
}

func (s *MetricsOpenTelemetry) unmarshalResourceMetrics(src []byte) (err error) {
	var (
		fc     easyproto.FieldContext
		resIdx = s.pushResource()
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1: // resource
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read resource (field %d)", fc.FieldNum)
			}
			if err := s.unmarshalResource(value, resIdx); err != nil {
				return errors.Wrap(err, "unmarshal resource")
			}
		case 2: // scope_metrics
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read scope_metrics (field %d)", fc.FieldNum)
			}
			if err := s.unmarshalScopeMetrics(value, resIdx); err != nil {
				return errors.Wrap(err, "unmarshal scope_metrics")
			}
		case 3: // schema_url
			value, ok := fc.String()
			if !ok {
				return errors.Errorf("read schema_url (field %d)", fc.FieldNum)
			}
			s.Metrics.Resources[resIdx].SchemaURL = value
		}
	}
	return nil
}

func (s *MetricsOpenTelemetry) unmarshalResource(src []byte, resIdx int) (err error) {
	var (
		fc  easyproto.FieldContext
		res = &s.Metrics.Resources[resIdx]
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1: // attributes
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read attributes (field %d)", fc.FieldNum)
			}
			nameIdx, v, err := s.unmarshalKeyValue(value)
			if err != nil {
				return errors.Wrap(err, "unmarshal attributes")
			}
			res.Attributes.Append(nameIdx, v)
		case 2: // dropped_attributes_count
			value, ok := fc.Uint32()
			if !ok {
				return errors.Errorf("read dropped_attributes_count (field %d)", fc.FieldNum)
			}
			res.DroppedAttributesCount = value
		}
	}
	return nil
}

func (s *MetricsOpenTelemetry) unmarshalScopeMetrics(src []byte, resIdx int) (err error) {
	var (
		fc       easyproto.FieldContext
		scopeIdx = s.pushScope()
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1: // scope
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read scope (field %d)", fc.FieldNum)
			}
			if err := s.unmarshalInstrumentationScope(value, scopeIdx); err != nil {
				return errors.Wrap(err, "unmarshal scope")
			}
		case 2: // metrics
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read metrics (field %d)", fc.FieldNum)
			}
			if err := s.unmarshalMetrics(value, resIdx, scopeIdx); err != nil {
				return errors.Wrap(err, "unmarshal metrics")
			}
		case 3: // schema_url
			value, ok := fc.String()
			if !ok {
				return errors.Errorf("read schema_url (field %d)", fc.FieldNum)
			}
			s.Metrics.Scopes[scopeIdx].SchemaURL = value
		}
	}
	return nil
}

func (s *MetricsOpenTelemetry) unmarshalInstrumentationScope(src []byte, scopeIdx int) (err error) {
	var (
		fc    easyproto.FieldContext
		scope = &s.Metrics.Scopes[scopeIdx]
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1: // name
			value, ok := fc.String()
			if !ok {
				return errors.Errorf("read name (field %d)", fc.FieldNum)
			}
			scope.Name = value
		case 2: // version
			value, ok := fc.String()
			if !ok {
				return errors.Errorf("read version (field %d)", fc.FieldNum)
			}
			scope.Version = value
		case 3: // attributes
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read attributes (field %d)", fc.FieldNum)
			}
			nameIdx, v, err := s.unmarshalKeyValue(value)
			if err != nil {
				return errors.Wrap(err, "unmarshal attributes")
			}
			scope.Attributes.Append(nameIdx, v)
		case 4: // dropped_attributes_count
			value, ok := fc.Uint32()
			if !ok {
				return errors.Errorf("read dropped_attributes_count (field %d)", fc.FieldNum)
			}
			scope.DroppedAttributesCount = value
		}
	}
	return nil
}

func (s *MetricsOpenTelemetry) unmarshalMetrics(src []byte, resIdx, scopeIdx int) (err error) {
	var (
		fc     easyproto.FieldContext
		oldsrc = src

		name, unit string
	)
	// Read name and unit first.
	{
		for len(src) > 0 {
			src, err = fc.NextField(src)
			if err != nil {
				return err
			}
			switch fc.FieldNum {
			case 1: // name
				v, ok := fc.String()
				if !ok {
					return errors.Errorf("read name (field %d)", fc.FieldNum)
				}
				name = v
			case 2: // description
			case 3: // unit
				v, ok := fc.String()
				if !ok {
					return errors.Errorf("read unit (field %d)", fc.FieldNum)
				}
				unit = v
			}
		}
	}

	// Restore state.
	src = oldsrc
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 5: // oneOf: gauge
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read gauge (field %d)", fc.FieldNum)
			}
			if err := s.unmarshalGauge(value, resIdx, scopeIdx, name, unit); err != nil {
				return errors.Wrap(err, "unmarshal gauge")
			}
		case 7: // oneOf: sum
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read Sum (field %d)", fc.FieldNum)
			}
			if err := s.unmarshalSum(value, resIdx, scopeIdx, name, unit); err != nil {
				return errors.Wrap(err, "unmarshal Sum")
			}
		case 9: // oneOf: histogram
		case 10: // oneOf: exponential_histogram
		case 11: // oneOf: summary
		}
	}

	return nil
}

func (s *MetricsOpenTelemetry) unmarshalGauge(src []byte, resIdx, scopeIdx int, name, unit string) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1: // data_points
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read data_points (field %d)", fc.FieldNum)
			}
			if err := s.unmarshalNumberDataPoint(value, resIdx, scopeIdx, name, unit); err != nil {
				return errors.Wrap(err, "unmarshal data_points")
			}
		}
	}
	return nil
}

func (s *MetricsOpenTelemetry) unmarshalSum(src []byte, resIdx, scopeIdx int, name, unit string) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1: // data_points
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read data_points (field %d)", fc.FieldNum)
			}
			if err := s.unmarshalNumberDataPoint(value, resIdx, scopeIdx, name, unit); err != nil {
				return errors.Wrap(err, "unmarshal data_points")
			}
		case 2: // aggregation_temporality
		case 3: // is_monotonic
		}
	}
	return nil
}

func (s *MetricsOpenTelemetry) unmarshalNumberDataPoint(src []byte, resIdx, scopeIdx int, name, unit string) (err error) {
	var (
		fc  easyproto.FieldContext
		idx = s.Number.Push()
	)

	h := s.Number.MetricHeader
	h.Resource[idx] = resIdx
	h.Scope[idx] = scopeIdx
	h.Name[idx] = name
	h.Unit[idx] = unit

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 7: // attributes
			value, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read attributes (field %d)", fc.FieldNum)
			}
			name, v, err := s.unmarshalKeyValue(value)
			if err != nil {
				return errors.Wrap(err, "unmarshal attributes")
			}
			if !v.is(pcommon.ValueTypeEmpty) {
				s.Number.Attributes[idx].Append(name, v)
			}
		case 2: // start_time_unix_nano
			value, ok := fc.Fixed64()
			if !ok {
				return errors.Errorf("read start_time_unix_nano (field %d)", fc.FieldNum)
			}
			s.Number.StartTimeUnixNano[idx] = value
		case 3: // time_unix_nano
			value, ok := fc.Fixed64()
			if !ok {
				return errors.Errorf("read time_unix_nano (field %d)", fc.FieldNum)
			}
			s.Number.TimeUnixNano[idx] = value
		case 4: // as_double
			value, ok := fc.Double()
			if !ok {
				return errors.Errorf("read as_double (field %d)", fc.FieldNum)
			}
			s.Number.Value[idx].SetDouble(value)
		case 6: // as_int
			value, ok := fc.Sfixed64()
			if !ok {
				return errors.Errorf("read as_int (field %d)", fc.FieldNum)
			}
			s.Number.Value[idx].SetInt(value)
		case 5: // exemplars
			_, ok := fc.MessageData()
			if !ok {
				return errors.Errorf("read exemplars (field %d)", fc.FieldNum)
			}
		case 8: // flags
			value, ok := fc.Uint32()
			if !ok {
				return errors.Errorf("read flags (field %d)", fc.FieldNum)
			}
			s.Number.Flags[idx] = pmetric.DataPointFlags(value)
		}
	}
	return nil
}

func (s *MetricsOpenTelemetry) unmarshalKeyValue(src []byte) (name string, v Value, err error) {
	var fc easyproto.FieldContext

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return name, v, err
		}
		switch fc.FieldNum {
		case 1: // key
			value, ok := fc.String()
			if !ok {
				return name, v, errors.Errorf("read key (field %d)", fc.FieldNum)
			}
			name = value
		case 2: // value
			value, ok := fc.MessageData()
			if !ok {
				return name, v, errors.Errorf("read value (field %d)", fc.FieldNum)
			}
			v, err = s.unmarshalAnyValue(value)
			if err != nil {
				return name, v, errors.Wrap(err, "unmarshal value")
			}
		}
	}
	return name, v, nil
}

func (s *MetricsOpenTelemetry) unmarshalAnyValue(src []byte) (v Value, err error) {
	var fc easyproto.FieldContext

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return v, err
		}
		switch fc.FieldNum {
		case 1: // string_value
			value, ok := fc.String()
			if !ok {
				return v, errors.Errorf("read string_value (field %d)", fc.FieldNum)
			}
			v.SetStr(value)
		case 2: // bool_value
			value, ok := fc.Bool()
			if !ok {
				return v, errors.Errorf("read bool_value (field %d)", fc.FieldNum)
			}
			v.SetBool(value)
		case 3: // int_value
			value, ok := fc.Int64()
			if !ok {
				return v, errors.Errorf("read int_value (field %d)", fc.FieldNum)
			}
			v.SetInt(value)
		case 4: // double_value
			value, ok := fc.Double()
			if !ok {
				return v, errors.Errorf("read double_value (field %d)", fc.FieldNum)
			}
			v.SetDouble(value)
		case 5: // array_value
			value, ok := fc.MessageData()
			if !ok {
				return v, errors.Errorf("read array_value (field %d)", fc.FieldNum)
			}
			v, err = s.unmarshalArrayValue(value)
			if err != nil {
				return v, errors.Wrap(err, "unmarshal array_value")
			}
		case 6: // kvlist_value
			value, ok := fc.MessageData()
			if !ok {
				return v, errors.Errorf("read kvlist_value (field %d)", fc.FieldNum)
			}
			v, err = s.unmarshalKeyValueList(value)
			if err != nil {
				return v, errors.Wrap(err, "unmarshal kvlist_value")
			}
		case 7: // bytes_value
			value, ok := fc.Bytes()
			if !ok {
				return v, errors.Errorf("read bytes_value (field %d)", fc.FieldNum)
			}
			v.SetBytes(value)
		}
	}
	return v, nil
}

func (s *MetricsOpenTelemetry) unmarshalKeyValueList(src []byte) (v Value, err error) {
	var (
		fc  easyproto.FieldContext
		arr []KeyValue
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return v, err
		}
		switch fc.FieldNum {
		case 1: // values
			value, ok := fc.MessageData()
			if !ok {
				return v, errors.Errorf("read values (field %d)", fc.FieldNum)
			}
			name, val, err := s.unmarshalKeyValue(value)
			if err != nil {
				return v, errors.Wrap(err, "unmarshal values")
			}
			arr = append(arr, KeyValue{
				Name:  name,
				Value: val,
			})
		}
	}
	v.SetMap(&s.Objects, arr)
	return v, nil
}

func (s *MetricsOpenTelemetry) unmarshalArrayValue(src []byte) (v Value, err error) {
	var (
		fc  easyproto.FieldContext
		arr []Value
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return v, err
		}
		switch fc.FieldNum {
		case 1: // values
			value, ok := fc.MessageData()
			if !ok {
				return v, errors.Errorf("read values (field %d)", fc.FieldNum)
			}
			elem, err := s.unmarshalAnyValue(value)
			if err != nil {
				return v, errors.Wrap(err, "unmarshal values")
			}
			arr = append(arr, elem)
		}
	}
	v.SetSlice(&s.Arrays, arr)
	return v, nil
}
