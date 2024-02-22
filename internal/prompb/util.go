package prompb

// Reset resets wr.
func (wr *WriteRequest) Reset() {
	for i := range wr.Timeseries {
		ts := &wr.Timeseries[i]
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]
	}
}
