package prompb

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
)

// WriteRequest represents Prometheus remote write API request
type WriteRequest struct {
	Timeseries []TimeSeries
}

// Unmarshal unmarshals WriteRequest from src.
func (req *WriteRequest) Unmarshal(src []byte) (err error) {
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
			req.Timeseries = append(req.Timeseries, TimeSeries{})
			ts := &req.Timeseries[len(req.Timeseries)-1]
			if err := ts.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read timeseries (field %d)", fc.FieldNum)
			}
		}
	}
	return nil
}
