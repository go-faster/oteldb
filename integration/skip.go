package integration

import (
	"os"
	"testing"
)

func Skip(t testing.TB) {
	t.Helper()
	if os.Getenv("E2E") == "" {
		t.Skip("Set E2E env to run")
	}
}
