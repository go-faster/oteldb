package otelschema

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/jx"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
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
	m := pcommon.NewMap()
	m.PutStr("http.request.method", "GET")
	m.PutInt("http.response.body.size", 123)
	m.PutStr("service.name", "foo")
	table.Append(otelstorage.Attrs(m))

	require.Equal(t, 1, table.Raw.Rows())
	data := table.Raw.Row(0)
	require.True(t, jx.Valid([]byte(data)), "invalid json: %s", data)
	t.Log(data)
	require.NotEmpty(t, table.Columns)
}

func BenchmarkTable_Append(b *testing.B) {
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
	m := pcommon.NewMap()
	m.PutStr("http.request.method", "GET")
	m.PutInt("http.response.body.size", 123)
	m.PutStr("service.name", "foo")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		table.Reset()
		table.Append(otelstorage.Attrs(m))
	}
}
