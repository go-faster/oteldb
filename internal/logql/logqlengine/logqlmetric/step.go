package logqlmetric

import "time"

type stepper struct {
	current time.Time
	end     time.Time
	step    time.Duration
}

func newStepper(
	start, end time.Time,
	step time.Duration,
) stepper {
	return stepper{
		current: start.Add(-step),
		end:     end,
		step:    step,
	}
}

func (s *stepper) next() (time.Time, bool) {
	s.current = s.current.Add(s.step)
	if s.current.After(s.end) {
		return s.current, false
	}
	return s.current, true
}
