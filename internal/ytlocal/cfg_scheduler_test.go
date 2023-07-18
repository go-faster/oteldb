package ytlocal

import "testing"

func TestScheduler(t *testing.T) {
	encode(t, "scheduler", Scheduler{
		BaseServer: newBaseServer(),
	})
}
