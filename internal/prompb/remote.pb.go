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
	var (
		fc  easyproto.FieldContext
		tss = req.Timeseries
	)
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
			if len(tss)+1 < cap(tss) {
				tss = tss[:len(tss)+1]
			} else {
				tss = append(tss, TimeSeries{})
			}
			ts := &tss[len(tss)-1]
			if err := ts.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read timeseries (field %d)", fc.FieldNum)
			}
		}
	}
	req.Timeseries = tss
	return nil
}
