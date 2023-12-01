package chotele2e

import (
	"context"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/jx"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap/zaptest"

	"github.com/go-faster/oteldb/internal/otelschema"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func TestSchema(t *testing.T) {
	table := otelschema.NewTable([]otelschema.AttributeInfo{
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

	var ddl strings.Builder
	ddl.WriteString("CREATE TABLE columns (")
	for _, c := range table.Columns {
		ddl.WriteString(c.Name)
		ddl.WriteString(" ")
		ddl.WriteString(c.Column.Type().String())
		ddl.WriteString(", ")
	}
	ddl.WriteString("raw String)")
	ddl.WriteString(" ENGINE Null")

	c := ConnectOpt(t, ch.Options{
		Logger: zaptest.NewLogger(t),
	})
	ctx := context.Background()
	require.NoError(t, c.Do(ctx, ch.Query{Body: ddl.String()}))

	input := table.Input()
	m := pcommon.NewMap()
	m.PutStr("http.request.method", "GET")
	m.PutInt("http.response.body.size", 123)
	m.PutStr("service.name", "foo")
	table.Append(otelstorage.Attrs(m))

	require.Equal(t, 1, table.Raw.Rows())
	data := table.Raw.Row(0)
	require.True(t, jx.Valid([]byte(data)))
	t.Log(data)

	require.NoError(t, c.Do(ctx, ch.Query{
		Body:  input.Into("columns"),
		Input: input,
	}))
}
