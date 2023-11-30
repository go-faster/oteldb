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

	-- for selects by trace_id, span_id to discover service_name, service_namespace and timestamp
	-- like SELECT service_name, service_namespace, timestamp FROM logs WHERE trace_id = '...'
	-- probably can be aggregated/grouped
	PROJECTION tracing (SELECT service_namespace, service_name, timestamp, trace_id, span_id ORDER BY trace_id, span_id)
)
  ENGINE = MergeTree
  PRIMARY KEY (service_namespace, service_name, toStartOfFiveMinutes(timestamp))
  ORDER BY (service_namespace, service_name, toStartOfFiveMinutes(timestamp), timestamp);`

	logAttrsSchema = `
CREATE TABLE IF NOT EXISTS %s
(
   name String,   -- foo_bar
   key  String,   -- foo.bar
)
   ENGINE = ReplacingMergeTree
   ORDER BY name;`
)
