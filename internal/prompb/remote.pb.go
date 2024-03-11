package prompb

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
)

// WriteRequest represents Prometheus remote write API request
type WriteRequest struct {
	Timeseries []TimeSeries
	pools      *pools
}

// Unmarshal unmarshals WriteRequest from src.
func (req *WriteRequest) Unmarshal(src []byte) (err error) {
	if req.pools == nil {
		req.pools = &pools{}
		req.pools.init()
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
			if err := ts.Unmarshal(req.pools, data); err != nil {
				return errors.Wrapf(err, "read timeseries (field %d)", fc.FieldNum)
			}
			req.Timeseries = append(req.Timeseries, ts)
		}
	}
	return nil
}
