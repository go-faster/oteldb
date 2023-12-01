package chstorage

import "github.com/ClickHouse/ch-go/proto"

type tableColumn struct {
	Name string
	Data proto.Column
}

type tableColumns []tableColumn

func (c tableColumns) Names() []string {
	var names []string
	for _, col := range c {
		names = append(names, col.Name)
	}
	return names
}

func (c tableColumns) Input() proto.Input {
	var cols proto.Input
	for _, col := range c {
		cols = append(cols, proto.InputColumn{
			Name: col.Name,
			Data: col.Data,
		})
	}
	return cols
}

func (c tableColumns) Result() proto.Results {
	var cols proto.Results
	for _, col := range c {
		cols = append(cols, proto.ResultColumn{
			Name: col.Name,
			Data: col.Data,
		})
	}
	return cols
}

func (c tableColumns) Reset() {
	for _, col := range c {
		col.Data.Reset()
	}
}
