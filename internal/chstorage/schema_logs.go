package chstorage

const (
	logsSchema = `
-- https://opentelemetry.io/docs/specs/otel/logs/data-model/#log-and-event-record-definition
(
	-- materialized fields from semantic conventions
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

	attribute String CODEC(ZSTD(1)),
	resource  LowCardinality(String) CODEC(ZSTD(1)),
	scope     LowCardinality(String) CODEC(ZSTD(1)),

	scope_name             LowCardinality(String),
	scope_version          LowCardinality(String),

    INDEX idx_trace_id trace_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_body body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
	INDEX idx_ts timestamp TYPE minmax GRANULARITY 8192,
	INDEX attribute_keys arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(scope), JSONExtractKeys(resource)) TYPE set(100),
)
  ENGINE = MergeTree
  PARTITION BY toYYYYMMDD(timestamp)
  PRIMARY KEY (severity_number, service_namespace, service_name, resource)
  ORDER BY (severity_number, service_namespace, service_name, resource, timestamp)
`

	logAttrsSchema = `
(
   name String,   -- foo_bar
   key  String,   -- foo.bar
)
   ENGINE = ReplacingMergeTree
   ORDER BY name`
)
