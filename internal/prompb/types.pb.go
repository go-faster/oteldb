package prompb

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
)

// TimeSeries is a timeseries.
type TimeSeries struct {
	Labels  []Label
	Samples []Sample
}

// Unmarshal unmarshals TimeSeries from src.
func (ts *TimeSeries) Unmarshal(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read label data")
			}
			var label Label
			if err := label.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read label (field %d)", fc.FieldNum)
			}
			ts.Labels = append(ts.Labels, label)
		case 2:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read sample data")
			}
			var sample Sample
			if err := sample.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read sample (field %d)", fc.FieldNum)
			}
			ts.Samples = append(ts.Samples, sample)
		}
	}
	return nil
}

// Label is a timeseries label
type Label struct {
	Name  []byte
	Value []byte
}

// Unmarshal unmarshals Label from src.
func (m *Label) Unmarshal(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			name, ok := fc.Bytes()
			if !ok {
				return errors.Wrapf(err, "read name (field %d)", fc.FieldNum)
			}
			m.Name = name
		case 2:
			value, ok := fc.Bytes()
			if !ok {
				return errors.Wrapf(err, "read value (field %d)", fc.FieldNum)
			}
			m.Value = value
		}
	}
	return nil
}

// Sample is a timeseries sample.
type Sample struct {
	Value     float64
	Timestamp int64
}

// Unmarshal unmarshals Sample from src.
func (s *Sample) Unmarshal(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			value, ok := fc.Double()
			if !ok {
				return errors.Wrapf(err, "read value (field %d)", fc.FieldNum)
			}
			s.Value = value
		case 2:
			timestamp, ok := fc.Int64()
			if !ok {
				return errors.Wrapf(err, "read timestamp (field %d)", fc.FieldNum)
			}
			s.Timestamp = timestamp
		}
	}
	return nil
}
