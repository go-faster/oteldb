package prompb

// Reset resets wr.
func (wr *WriteRequest) Reset() {
	for i := range wr.Timeseries {
		wr.Timeseries[i].Reset()
	}
	wr.Timeseries = wr.Timeseries[:0]
}

// Reset resets [TimeSeries] fields.
func (ts *TimeSeries) Reset() {
	ts.Labels = ts.Labels[:0]
	ts.Samples = ts.Samples[:0]

	for i := range ts.Exemplars {
		ts.Exemplars[i].Reset()
	}
	ts.Exemplars = ts.Exemplars[:0]

	for i := range ts.Histograms {
		ts.Histograms[i].Reset()
	}
	ts.Histograms = ts.Histograms[:0]
}

// Reset resets [Exemplar] fields.
func (e *Exemplar) Reset() {
	e.Labels = e.Labels[:0]
}

// Reset resets [Histogram] fields.
func (h *Histogram) Reset() {
	h.NegativeSpans = h.NegativeSpans[:0]
	h.NegativeDeltas = h.NegativeDeltas[:0]
	h.NegativeCounts = h.NegativeCounts[:0]

	h.PositiveSpans = h.PositiveSpans[:0]
	h.PositiveDeltas = h.PositiveDeltas[:0]
	h.PositiveCounts = h.PositiveCounts[:0]
}
