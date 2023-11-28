package chstorage

const (
	logsSchema = `
-- https://opentelemetry.io/docs/specs/otel/logs/data-model/#log-and-event-record-definition
CREATE TABLE IF NOT EXISTS %s
(
	-- materialized fields from semantic conventions
	-- NB: They MUST NOT be present in the 'resource' field.
	service_instance_id LowCardinality(String) COMMENT 'service.instance.id',
	service_name        LowCardinality(String) COMMENT 'service.name',
	service_namespace   LowCardinality(String) COMMENT 'service.namespace',

	-- Timestamp, or ObservedTimestamp if not present.
	timestamp          DateTime64(9) CODEC(DoubleDelta),

	-- Severity Fields
	severity_text      LowCardinality(String), -- SeverityText
	severity_number    UInt8,                  -- SeverityNumber [1, 24]

	-- Trace Context Fields
	trace_id           FixedString(16),  -- TraceId
	span_id            FixedString(8),   -- SpanId
	trace_flags        UInt8,            -- TraceFlags

	-- can be arbitrary json
	body               String, -- json
	attributes         String, -- json object
	resource           String, -- json object

	scope_name         LowCardinality(String),
	scope_version      LowCardinality(String),
	scope_attributes   String, -- json object
)
  ENGINE = MergeTree()
  ORDER BY (service_namespace, service_name, service_instance_id, toStartOfFiveMinutes(timestamp));`

	logAttrsSchema = `
CREATE TABLE IF NOT EXISTS %s
(
   name String,   -- foo_bar
   key  String,   -- foo.bar
)
   ENGINE = ReplacingMergeTree
   ORDER BY name;`
)
