// Package integration wraps integration
package integration

import (
	"os"
	"testing"
)

// Skip test if E2E env is not set.
func Skip(t testing.TB) {
	t.Helper()
	if os.Getenv("E2E") == "" {
		t.Skip("Set E2E env to run")
	}
}
