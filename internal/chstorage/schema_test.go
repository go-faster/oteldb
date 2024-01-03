package chstorage

import (
	"testing"
	"time"

	"github.com/go-faster/sdk/gold"
)

func TestGenerateDDL(t *testing.T) {
	tables := DefaultTables()
	tables.TTL = time.Hour * 72

	out := tables.generateQuery(generateOptions{
		Name:     "trace_spans",
		TTLField: "start",
		DDL:      spansSchema,
	})

	gold.Str(t, out, "trace_spans.sql")
}
