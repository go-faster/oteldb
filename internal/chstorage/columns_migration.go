package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"
)

type migrationColumns struct {
	table proto.ColStr // table name
	ddl   proto.ColStr // SHA256(DDL)
}

func (c *migrationColumns) columns() Columns {
	return []Column{
		{Name: "table", Data: &c.table},
		{Name: "ddl", Data: &c.ddl},
	}
}

func (c *migrationColumns) Input() proto.Input    { return c.columns().Input() }
func (c *migrationColumns) Result() proto.Results { return c.columns().Result() }

func newMigrationColumns() *migrationColumns {
	return &migrationColumns{}
}

func (c *migrationColumns) Mapping() map[string]string {
	data := make(map[string]string, c.table.Rows())
	for i := 0; i < c.table.Rows(); i++ {
		data[c.table.Row(i)] = c.ddl.Row(i)
	}
	return data
}

func (c *migrationColumns) Save(m map[string]string) {
	for k, v := range m {
		c.table.Append(k)
		c.ddl.Append(v)
	}
}
