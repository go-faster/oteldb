package prompb

// GetTimestamp returns Timestamp.
func (h *Sample) GetTimestamp() int64 {
	if h == nil {
		return 0
	}
	return h.Timestamp
}

// GetTimestamp returns Timestamp.
func (h *Exemplar) GetTimestamp() int64 {
	if h == nil {
		return 0
	}
	return h.Timestamp
}

// GetTimestamp returns Timestamp.
func (h *Histogram) GetTimestamp() int64 {
	if h == nil {
		return 0
	}
	return h.Timestamp
}
