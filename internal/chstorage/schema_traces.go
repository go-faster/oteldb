package chstorage

const (
	spansSchema = `
(
	-- materialized fields from semantic conventions
	service_instance_id LowCardinality(String) COMMENT 'service.instance.id',
	service_name        LowCardinality(String) COMMENT 'service.name',
	service_namespace   LowCardinality(String) COMMENT 'service.namespace',

	-- Trace Context Fields
	trace_id           FixedString(16) CODEC(ZSTD(1)),  -- TraceId
	span_id            FixedString(8)  CODEC(ZSTD(1)),  -- SpanId

	trace_state String,
	parent_span_id FixedString(8),
	name LowCardinality(String),
	kind Enum8(` + kindDDL + `),

	start DateTime64(9) CODEC(Delta, ZSTD(1)),
	end   DateTime64(9) CODEC(Delta, ZSTD(1)),
	duration_ns UInt64 Materialized toUnixTimestamp64Nano(end)-toUnixTimestamp64Nano(start) CODEC(T64, ZSTD(1)),

	status_code UInt8 CODEC(T64, ZSTD(1)),
	status_message LowCardinality(String),

	batch_id UUID,

	attribute LowCardinality(String),
	resource  LowCardinality(String),
	scope     LowCardinality(String),

	scope_name             LowCardinality(String),
	scope_version          LowCardinality(String),

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
PRIMARY KEY (service_namespace, service_name, resource)
ORDER BY (service_namespace, service_name, resource, start)
`
	kindDDL    = `'KIND_UNSPECIFIED' = 0,'KIND_INTERNAL' = 1,'KIND_SERVER' = 2,'KIND_CLIENT' = 3,'KIND_PRODUCER' = 4,'KIND_CONSUMER' = 5`
	tagsSchema = `
	(
		name LowCardinality(String),
		value String,
		value_type Enum8(` + valueTypeDDL + `)
	)
	ENGINE = ReplacingMergeTree
	ORDER BY (value_type, name, value);`
	valueTypeDDL = `'EMPTY' = 0,'STR' = 1,'INT' = 2,'DOUBLE' = 3,'BOOL' = 4,'MAP' = 5,'SLICE' = 6,'BYTES' = 7`
)
