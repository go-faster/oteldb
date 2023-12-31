package ddl

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	s, err := Generate(Table{
		Engine:  "MergeTree()",
		OrderBy: []string{"a", "b"},
		Columns: []Column{
			{
				Name: "a",
				Type: "Int32",
			},
			{
				Name:  "b",
				Type:  proto.ColumnType("LowCardinality").Sub(proto.ColumnTypeString),
				Codec: "ZSTD(1)",
			},
		},
	})
	require.NoError(t, err)
	gold.Str(t, s, "ddl.txt")
}
