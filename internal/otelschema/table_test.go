package otelschema

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestNewTable(t *testing.T) {
	table := NewTable([]AttributeInfo{
		{
			Name: "http.request.method",
			Type: proto.ColumnTypeString,
		},
		{
			Name: "http.response.body.size",
			Type: proto.ColumnTypeInt64,
		},
	})
	require.NotEmpty(t, table.Columns)
}
