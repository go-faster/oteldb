package chstorage

const (
	logsSchema = `
-- https://opentelemetry.io/docs/specs/otel/logs/data-model/#log-and-event-record-definition
(
	-- materialized fields from semantic conventions
	-- NB: They MUST NOT be present in the 'resource' field.
	service_instance_id LowCardinality(String) COMMENT 'service.instance.id',
	service_name        LowCardinality(String) COMMENT 'service.name',
	service_namespace   LowCardinality(String) COMMENT 'service.namespace',

	-- Timestamp, or ObservedTimestamp if not present.
	timestamp          DateTime64(9) CODEC(Delta, ZSTD(1)),

	-- Severity Fields
	severity_text      LowCardinality(String), -- SeverityText
	severity_number    UInt8,                  -- SeverityNumber [1, 24]

	-- Trace Context Fields
	trace_id           FixedString(16) CODEC(ZSTD(1)),  -- TraceId
	span_id            FixedString(8)  CODEC(ZSTD(1)),  -- SpanId
	trace_flags        UInt8 CODEC(T64, ZSTD(1)),       -- TraceFlags

	body               String CODEC(ZSTD(1)), -- string or json object

	attributes         Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
	attributes_types   Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]
	resource           Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
	resource_types     Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]

	scope_name             LowCardinality(String),
	scope_version          LowCardinality(String),
	scope_attributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),  -- string[str | json]
	scope_attributes_types Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)),  -- string[type]

    INDEX idx_trace_id trace_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_body body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
	INDEX idx_ts timestamp TYPE minmax GRANULARITY 8192,
)
  ENGINE = MergeTree
  PARTITION BY toYYYYMMDD(timestamp)
  PRIMARY KEY (severity_number, service_namespace, service_name, cityHash64(resource))
  ORDER BY (severity_number, service_namespace, service_name, cityHash64(resource), timestamp)
`

	logAttrsSchema = `
(
   name String,   -- foo_bar
   key  String,   -- foo.bar
)
   ENGINE = ReplacingMergeTree
   ORDER BY name`
)
