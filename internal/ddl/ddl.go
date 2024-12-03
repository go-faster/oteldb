// Package ddl provides facilities for generating DDL statements and auxiliary code.
package ddl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
)

// Column of [Table].
type Column struct {
	Name    string
	Comment string
	Type    proto.ColumnType
	Codec   proto.ColumnType
}

// Table description.
type Table struct {
	Name        string
	Columns     []Column
	Indexes     []Index
	OrderBy     []string
	PrimaryKey  []string
	PartitionBy string
	Engine      string
}

type Index struct {
	Name        string
	Target      string
	Type        string
	Params      []string
	Granularity int
}

func (i Index) string(nameFormat string) string {
	var b strings.Builder
	b.WriteString("INDEX ")
	b.WriteString(fmt.Sprintf(nameFormat, Backtick(i.Name)))
	b.WriteString(" ")
	b.WriteString(i.Target)
	b.WriteString(" TYPE ")
	b.WriteString(i.Type)
	if len(i.Params) > 0 {
		b.WriteString("(")
		b.WriteString(strings.Join(i.Params, ", "))
		b.WriteString(")")
	}
	if i.Granularity > 0 {
		b.WriteString(" GRANULARITY ")
		b.WriteString(strconv.Itoa(i.Granularity))
	}
	return b.String()
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
	b.WriteString(`CREATE TABLE IF NOT EXISTS `)
	if table.Name == "" {
		b.WriteString("table")
	} else {
		b.WriteString(Backtick(table.Name))
	}
	b.WriteString("\n(")
	var (
		maxColumnLen     int
		maxColumnTypeLen int
	)
	for _, c := range table.Columns {
		if len(c.Name) > maxColumnLen {
			maxColumnLen = len(c.Name)
		}
		if len(c.Type.String()) > maxColumnTypeLen {
			maxColumnTypeLen = len(c.Type.String())
		}
	}
	for _, c := range table.Columns {
		b.WriteString("\n")
		var col strings.Builder
		col.WriteString("\t")
		nameFormat := "%-" + strconv.Itoa(maxColumnLen+2) + "s"
		col.WriteString(fmt.Sprintf(nameFormat, Backtick(c.Name)))
		col.WriteString(" ")
		typeFormat := "%-" + strconv.Itoa(maxColumnTypeLen) + "s"
		if c.Codec == "" && c.Comment == "" {
			typeFormat = "%s"
		}
		col.WriteString(fmt.Sprintf(typeFormat, c.Type.String()))
		if c.Comment != "" {
			col.WriteString(" COMMENT ")
			col.WriteString("'")
			col.WriteString(c.Comment)
			col.WriteString("'")
		}
		if c.Codec != "" {
			col.WriteString(" CODEC(")
			col.WriteString(c.Codec.String())
			col.WriteRune(')')
		}
		col.WriteString(",")
		b.WriteString(col.String())
	}
	var maxIndexLen int
	for _, c := range table.Indexes {
		if len(c.Name) > maxIndexLen {
			maxIndexLen = len(c.Name)
		}
	}
	for i, c := range table.Indexes {
		if i == 0 {
			b.WriteString("\n")
		}
		b.WriteString("\n\t")
		nameFormat := "%-" + strconv.Itoa(maxIndexLen+2) + "s"
		b.WriteString(c.string(nameFormat))
		b.WriteString(",")
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
