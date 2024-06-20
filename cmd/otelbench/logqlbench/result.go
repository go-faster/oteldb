package logqlbench

import "github.com/go-faster/oteldb/cmd/otelbench/chtracker"

type LogQLReport struct {
	Queries []LogQLReportQuery `json:"queries"`
}

type LogQLReportQuery struct {
	ID            int                     `yaml:"id,omitempty"`
	Query         string                  `yaml:"query,omitempty"`
	Title         string                  `yaml:"title,omitempty"`
	Description   string                  `yaml:"description,omitempty"`
	DurationNanos int64                   `yaml:"duration_nanos,omitempty"`
	Matchers      []string                `yaml:"matchers,omitempty"`
	Queries       []chtracker.QueryReport `yaml:"queries,omitempty"`
}
