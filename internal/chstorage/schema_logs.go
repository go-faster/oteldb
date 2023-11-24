package chstorage

const (
	logsSchema = `CREATE TABLE IF NOT EXISTS %s
(
	timestamp          DateTime64(9),
	observed_timestamp DateTime64(9),
	flags              UInt32,
	severity_text      String,
	severity_number    Int32,
	body               String,
	trace_id           FixedString(16),
	span_id            FixedString(8),
	attributes         String,
	resource           String,
	scope_name         String,
	scope_version      String,
	scope_attributes   String,
)
ENGINE = MergeTree()
ORDER BY (timestamp);`
)
