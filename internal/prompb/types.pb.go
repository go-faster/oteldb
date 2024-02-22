package prompb

import (
	"math"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
)

// TimeSeries is a timeseries.
type TimeSeries struct {
	Labels     []Label
	Samples    []Sample
	Exemplars  []Exemplar
	Histograms []Histogram
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
				return errors.New("read labels data")
			}
			var label Label
			if err := label.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read labels (field %d)", fc.FieldNum)
			}
			ts.Labels = append(ts.Labels, label)
		case 2:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read samples data")
			}
			var sample Sample
			if err := sample.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read samples (field %d)", fc.FieldNum)
			}
			ts.Samples = append(ts.Samples, sample)
		case 3:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read exemplars data")
			}
			var exemplar Exemplar
			if err := exemplar.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read exemplars (field %d)", fc.FieldNum)
			}
			ts.Exemplars = append(ts.Exemplars, exemplar)
		case 4:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read histograms data")
			}
			var histogram Histogram
			if err := histogram.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read histograms (field %d)", fc.FieldNum)
			}
			ts.Histograms = append(ts.Histograms, histogram)
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
				return errors.Errorf("read name (field %d)", fc.FieldNum)
			}
			m.Name = name
		case 2:
			value, ok := fc.Bytes()
			if !ok {
				return errors.Errorf("read value (field %d)", fc.FieldNum)
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
				return errors.Errorf("read value (field %d)", fc.FieldNum)
			}
			s.Value = value
		case 2:
			timestamp, ok := fc.Int64()
			if !ok {
				return errors.Errorf("read timestamp (field %d)", fc.FieldNum)
			}
			s.Timestamp = timestamp
		}
	}
	return nil
}

type Exemplar struct {
	Labels    []Label
	Value     float64
	Timestamp int64
}

// Unmarshal unmarshals Exemplar from src.
func (e *Exemplar) Unmarshal(src []byte) (err error) {
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
				return errors.New("read labels data")
			}
			var label Label
			if err := label.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read labels (field %d)", fc.FieldNum)
			}
			e.Labels = append(e.Labels, label)
		case 2:
			value, ok := fc.Double()
			if !ok {
				return errors.Errorf("read value (field %d)", fc.FieldNum)
			}
			e.Value = value
		case 3:
			timestamp, ok := fc.Int64()
			if !ok {
				return errors.Errorf("read timestamp (field %d)", fc.FieldNum)
			}
			e.Timestamp = timestamp
		}
	}
	return nil
}

type Histogram struct {
	Count         HistogramCount
	Sum           float64
	Schema        int32
	ZeroThreshold float64
	ZeroCount     HistogramZeroCount

	NegativeSpans  []BucketSpan
	NegativeDeltas []int64
	NegativeCounts []float64

	PositiveSpans  []BucketSpan
	PositiveDeltas []int64
	PositiveCounts []float64

	ResetHint HistogramResentHint
	Timestamp int64
}

// Unmarshal unmarshals BucketSpan from src.
func (h *Histogram) Unmarshal(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			count, ok := fc.Uint64()
			if !ok {
				return errors.Errorf("read count_int (field %d)", fc.FieldNum)
			}
			h.Count.SetInt(count)
		case 2:
			count, ok := fc.Double()
			if !ok {
				return errors.Errorf("read count_float (field %d)", fc.FieldNum)
			}
			h.Count.SetFloat(count)
		case 3:
			sum, ok := fc.Double()
			if !ok {
				return errors.Errorf("read sum (field %d)", fc.FieldNum)
			}
			h.Sum = sum
		case 4:
			schema, ok := fc.Sint32()
			if !ok {
				return errors.Errorf("read schema (field %d)", fc.FieldNum)
			}
			h.Schema = schema
		case 5:
			zeroThreshold, ok := fc.Double()
			if !ok {
				return errors.Errorf("read zero_threshold (field %d)", fc.FieldNum)
			}
			h.ZeroThreshold = zeroThreshold
		case 6:
			zeroCount, ok := fc.Uint64()
			if !ok {
				return errors.Errorf("read zero_count_int (field %d)", fc.FieldNum)
			}
			h.ZeroCount.SetInt(zeroCount)
		case 7:
			zeroCount, ok := fc.Double()
			if !ok {
				return errors.Errorf("read zero_count_float (field %d)", fc.FieldNum)
			}
			h.ZeroCount.SetFloat(zeroCount)
		case 8:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read negative_spans data")
			}
			var span BucketSpan
			if err := span.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read negative_spans (field %d)", fc.FieldNum)
			}
			h.NegativeSpans = append(h.NegativeSpans, span)
		case 9:
			deltas, ok := fc.UnpackSint64s(h.NegativeDeltas)
			if !ok {
				return errors.Errorf("read negative_deltas (field %d)", fc.FieldNum)
			}
			h.NegativeDeltas = deltas
		case 10:
			counts, ok := fc.UnpackDoubles(h.NegativeCounts)
			if !ok {
				return errors.Errorf("read negative_counts (field %d)", fc.FieldNum)
			}
			h.NegativeCounts = counts
		case 11:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read positive_spans data")
			}
			var span BucketSpan
			if err := span.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read positive_spans (field %d)", fc.FieldNum)
			}
			h.PositiveSpans = append(h.PositiveSpans, span)
		case 12:
			deltas, ok := fc.UnpackSint64s(h.PositiveDeltas)
			if !ok {
				return errors.Errorf("read positive_deltas (field %d)", fc.FieldNum)
			}
			h.PositiveDeltas = deltas
		case 13:
			counts, ok := fc.UnpackDoubles(h.PositiveCounts)
			if !ok {
				return errors.Errorf("read positive_counts (field %d)", fc.FieldNum)
			}
			h.PositiveCounts = counts
		case 14:
			hint, ok := fc.Int32()
			if !ok {
				return errors.Errorf("read hint (field %d)", fc.FieldNum)
			}
			h.ResetHint = HistogramResentHint(hint)
		case 15:
			timestamp, ok := fc.Int64()
			if !ok {
				return errors.Errorf("read timestamp (field %d)", fc.FieldNum)
			}
			h.Timestamp = timestamp
		}
	}
	return nil
}

type (
	HistogramCount     = intOrFloat
	HistogramZeroCount = intOrFloat
)

type intOrFloat struct {
	data  uint64
	isInt bool
}

func (h *intOrFloat) SetInt(data uint64) {
	h.data = data
	h.isInt = true
}

func (h *intOrFloat) SetFloat(data float64) {
	h.data = math.Float64bits(data)
	h.isInt = false
}

func (h intOrFloat) AsUint64() (uint64, bool) {
	return h.data, h.isInt
}

func (h intOrFloat) AsFloat64() (float64, bool) {
	return math.Float64frombits(h.data), !h.isInt
}

type BucketSpan struct {
	Offset int32
	Length uint32
}

// Unmarshal unmarshals BucketSpan from src.
func (s *BucketSpan) Unmarshal(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			offset, ok := fc.Sint32()
			if !ok {
				return errors.Errorf("read offset (field %d)", fc.FieldNum)
			}
			s.Offset = offset
		case 2:
			length, ok := fc.Uint32()
			if !ok {
				return errors.Errorf("read length (field %d)", fc.FieldNum)
			}
			s.Length = length
		}
	}
	return nil
}

type HistogramResentHint int32

const (
	HistogramResentHintUNKNOWN HistogramResentHint = 0
	HistogramResentHintYES     HistogramResentHint = 1
	HistogramResentHintNO      HistogramResentHint = 2
	HistogramResentHintGAUGE   HistogramResentHint = 3
)
