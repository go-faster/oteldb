package logqlbench

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
	Queries       []ClickhouseQueryReport `yaml:"queries,omitempty"`
}

type ClickhouseQueryReport struct {
	DurationNanos int64  `yaml:"duration_nanos,omitempty"`
	Query         string `yaml:"query,omitempty"`
	ReadBytes     int64  `yaml:"read_bytes,omitempty"`
	ReadRows      int64  `yaml:"read_rows,omitempty"`
	MemoryUsage   int64  `yaml:"memory_usage,omitempty"`

	ReceivedBytes int64 `yaml:"recevied_bytes,omitempty"`
	ReceivedRows  int64 `yaml:"recevied_rows,omitempty"`
}
