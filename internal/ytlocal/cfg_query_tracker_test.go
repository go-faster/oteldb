package ytlocal

import "testing"

func TestQueryTracker(t *testing.T) {
	encode(t, "query-proxy", QueryTracker{
		BaseServer: newBaseServer(),
		User:       "query-tracker",
	})
}
