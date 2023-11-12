package promhandler

import "time"

// PromAPIOptions describes [PromAPI] options.
type PromAPIOptions struct {
	// LookbackDelta sets default lookback delta. Defaults to [time.Hour].
	LookbackDelta time.Duration
}

func (opts *PromAPIOptions) setDefaults() {
	if opts.LookbackDelta == 0 {
		opts.LookbackDelta = 5 * time.Minute
	}
}
