// Package ddl provides facilities for generating DDL statements and auxiliary code.
package ddl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// Column of [Table].
type Column struct {
	Name         string
	Comment      string
	Default      string
	Type         proto.ColumnType
	Codec        proto.ColumnType
	Materialized string
}

// Table description.
type Table struct {
	Name        string
	Cluster     string
	Columns     []Column
	Indexes     []Index
	OrderBy     []string
	PrimaryKey  []string
	PartitionBy string
	Engine      string
	TTL         TTL
}

type TTL struct {
	Field string
	Delta time.Duration
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
func Generate(t Table) (string, error) {
	var b strings.Builder
	b.WriteString(`CREATE TABLE IF NOT EXISTS `)
	if t.Name == "" {
		b.WriteString(Backtick("table"))
	} else {
		b.WriteString(Backtick(t.Name))
	}
	if t.Cluster != "" {
		b.WriteString(" ON CLUSTER ")
		b.WriteString(Backtick(t.Cluster))
	}
	b.WriteString("\n(")
	var (
		maxColumnLen     int
		maxColumnTypeLen int
	)
	for _, c := range t.Columns {
		if len(c.Name) > maxColumnLen {
			maxColumnLen = len(c.Name)
		}
		if len(c.Type.String()) > maxColumnTypeLen && !strings.HasPrefix(c.Type.String(), "Enum") {
			maxColumnTypeLen = len(c.Type.String())
		}
	}
	hasIndexes := len(t.Indexes) > 0
	for i, c := range t.Columns {
		b.WriteString("\n")
		var col strings.Builder
		col.WriteString("\t")
		if c.Name == "" {
			// Comment row.
			col.WriteString("-- ")
			col.WriteString(c.Comment)
			b.WriteString(col.String())
			continue
		}
		nameFormat := "%-" + strconv.Itoa(maxColumnLen+2) + "s"
		col.WriteString(fmt.Sprintf(nameFormat, Backtick(c.Name)))
		col.WriteString(" ")
		typeFormat := "%-" + strconv.Itoa(maxColumnTypeLen) + "s"
		if c.Codec == "" && c.Comment == "" {
			typeFormat = "%s"
		}
		col.WriteString(fmt.Sprintf(typeFormat, c.Type.String()))
		if c.Materialized != "" {
			col.WriteString(" MATERIALIZED ")
			col.WriteString(c.Materialized)
		}
		if c.Default != "" {
			col.WriteString(" DEFAULT ")
			col.WriteString(c.Default)
		}
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
		b.WriteString(col.String())
		if hasIndexes || i < len(t.Columns)-1 {
			b.WriteString(",")
		}
	}
	var maxIndexLen int
	for _, c := range t.Indexes {
		if len(c.Name) > maxIndexLen {
			maxIndexLen = len(c.Name)
		}
	}
	for i, c := range t.Indexes {
		if i == 0 {
			b.WriteString("\n")
		}
		b.WriteString("\n\t")
		nameFormat := "%-" + strconv.Itoa(maxIndexLen+2) + "s"
		b.WriteString(c.string(nameFormat))
		if i < len(t.Indexes)-1 {
			b.WriteString(",")
		}
	}
	b.WriteString("\n)\n")
	b.WriteString("ENGINE = ")
	if t.Engine == "" {
		t.Engine = "Null"
	} else {
		b.WriteString(t.Engine)
	}
	b.WriteString("\n")
	if t.PartitionBy != "" {
		b.WriteString("PARTITION BY ")
		b.WriteString(t.PartitionBy)
		b.WriteString("\n")
	}
	if len(t.OrderBy) > 0 {
		b.WriteString("ORDER BY (")
		b.WriteString(strings.Join(backticks(t.OrderBy), ", "))
		b.WriteString(")\n")
	}
	if len(t.PrimaryKey) > 0 {
		b.WriteString("PRIMARY KEY (")
		b.WriteString(strings.Join(backticks(t.PrimaryKey), ", "))
		b.WriteString(")\n")
	}
	if t.TTL.Delta > 0 && t.TTL.Field != "" {
		b.WriteString(fmt.Sprintf("TTL toDateTime(%s) + toIntervalSecond(%d)",
			Backtick(t.TTL.Field), t.TTL.Delta/time.Second,
		))
	}
	return b.String(), nil
}
