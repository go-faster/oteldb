package chstorage

import (
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/ddl"
)

func Test_logColumns_DDL(t *testing.T) {
	lc := newLogColumns()
	table := lc.DDL()

	s, err := ddl.Generate(table)
	require.NoError(t, err)

	gold.Str(t, s, "log_columns_ddl.sql")
}
