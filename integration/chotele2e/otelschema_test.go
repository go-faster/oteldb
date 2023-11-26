package chotele2e

import (
	"context"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/otelschema"
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

	c := Connect(t)
	ctx := context.Background()
	require.NoError(t, c.Do(ctx, ch.Query{Body: ddl.String()}))

	input := table.Input()
	require.NoError(t, c.Do(ctx, ch.Query{
		Body:  input.Into("columns"),
		Input: input,
	}))
}
