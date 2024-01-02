// Package ddl provides facilities for generating DDL statements and auxiliary code.
package ddl

import (
	"strings"

	"github.com/ClickHouse/ch-go/proto"
)

// Column of [Table].
type Column struct {
	Name  string
	Type  proto.ColumnType
	Codec proto.ColumnType
}

// Table description.
type Table struct {
	Name        string
	Columns     []Column
	OrderBy     []string
	PrimaryKey  []string
	PartitionBy string
	Engine      string
}

// Backtick adds backticks to the string.
func Backtick(s string) string {
	return "`" + s + "`"
}

func backticks(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = Backtick(s)
	}
	return out
}

// Generate DDL without CREATE TABLE statement.
func Generate(table Table) (string, error) {
	var b strings.Builder
	b.WriteString("(\n")
	for i, c := range table.Columns {
		var col strings.Builder
		col.WriteString("\t")
		col.WriteString(Backtick(c.Name))
		col.WriteString(" ")
		col.WriteString(c.Type.String())
		if c.Codec != "" {
			col.WriteString(" CODEC(")
			col.WriteString(c.Codec.String())
			col.WriteRune(')')
		}
		if i < len(table.Columns)-1 {
			col.WriteString(",\n")
		}
		b.WriteString(col.String())
	}
	b.WriteString("\n)\n")
	b.WriteString("ENGINE = ")
	if table.Engine == "" {
		table.Engine = "Null"
	} else {
		b.WriteString(table.Engine)
	}
	b.WriteString("\n")
	if table.PartitionBy != "" {
		b.WriteString("PARTITION BY ")
		b.WriteString(table.PartitionBy)
		b.WriteString("\n")
	}
	if len(table.OrderBy) > 0 {
		b.WriteString("ORDER BY (")
		b.WriteString(strings.Join(backticks(table.OrderBy), ", "))
		b.WriteString(")\n")
	}
	if len(table.PrimaryKey) > 0 {
		b.WriteString("PRIMARY KEY (")
		b.WriteString(strings.Join(backticks(table.PrimaryKey), ", "))
		b.WriteString(")\n")
	}
	return b.String(), nil
}
