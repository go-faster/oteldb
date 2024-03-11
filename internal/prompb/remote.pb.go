package prompb

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
)

// WriteRequest represents Prometheus remote write API request
type WriteRequest struct {
	Timeseries []TimeSeries
	Pools      *Pools
}

// Unmarshal unmarshals WriteRequest from src.
func (req *WriteRequest) Unmarshal(src []byte) (err error) {
	if req.Pools == nil {
		req.Pools = &Pools{
			Labels:                  new(slicepool[Label]),
			Samples:                 new(slicepool[Sample]),
			Exemplars:               new(slicepool[Exemplar]),
			ExemplarLabels:          new(slicepool[Label]),
			Histograms:              new(slicepool[Histogram]),
			HistogramNegativeSpans:  new(slicepool[BucketSpan]),
			HistogramNegativeDeltas: new(slicepool[int64]),
			HistogramNegativeCounts: new(slicepool[float64]),
			HistogramPositiveSpans:  new(slicepool[BucketSpan]),
			HistogramPositiveDeltas: new(slicepool[int64]),
			HistogramPositiveCounts: new(slicepool[float64]),
		}
	}

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
				return errors.New("read timeseries data")
			}
			var ts TimeSeries
			if err := ts.Unmarshal(req.Pools, data); err != nil {
				return errors.Wrapf(err, "read timeseries (field %d)", fc.FieldNum)
			}
			req.Timeseries = append(req.Timeseries, ts)
		}
	}
	return nil
}
