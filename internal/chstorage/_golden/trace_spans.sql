CREATE TABLE IF NOT EXISTS `trace_spans`
(
	-- materialized fields from semantic conventions
	-- NB: They MUST NOT be present in the 'resource' field.
	service_instance_id LowCardinality(String) COMMENT 'service.instance.id',
	service_name        LowCardinality(String) COMMENT 'service.name',
	service_namespace   LowCardinality(String) COMMENT 'service.namespace',

	-- Trace Context Fields
	trace_id           FixedString(16) CODEC(ZSTD(1)),  -- TraceId
	span_id            FixedString(8)  CODEC(ZSTD(1)),  -- SpanId

	trace_state String,
	parent_span_id FixedString(8),
	name LowCardinality(String),
	kind Enum8('KIND_UNSPECIFIED' = 0,'KIND_INTERNAL' = 1,'KIND_SERVER' = 2,'KIND_CLIENT' = 3,'KIND_PRODUCER' = 4,'KIND_CONSUMER' = 5),

	start DateTime64(9) CODEC(Delta, ZSTD(1)),
	end   DateTime64(9) CODEC(Delta, ZSTD(1)),
	duration_ns UInt64 Materialized toUnixTimestamp64Nano(end)-toUnixTimestamp64Nano(start) CODEC(T64, ZSTD(1)),

	status_code UInt8 CODEC(T64, ZSTD(1)),
	status_message LowCardinality(String),

	batch_id UUID,
	attributes         Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
	attributes_types   Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]
	resource           Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- string[str | json]
	resource_types     Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)), -- string[type]

	scope_name             LowCardinality(String),
	scope_version          LowCardinality(String),
	scope_attributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),  -- string[str | json]
	scope_attributes_types Map(LowCardinality(String), UInt8)  CODEC(ZSTD(5)),  -- string[type]

	events_timestamps Array(DateTime64(9)),
	events_names Array(String),
	events_attributes Array(String),

	links_trace_ids Array(FixedString(16)),
	links_span_ids Array(FixedString(8)),
	links_tracestates Array(String),
	links_attributes Array(String),

	INDEX idx_trace_id trace_id TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(start)
PRIMARY KEY (service_namespace, service_name, cityHash64(resource))
ORDER BY (service_namespace, service_name, cityHash64(resource), start)
TTL toDateTime(`start`) + toIntervalSecond(259200)