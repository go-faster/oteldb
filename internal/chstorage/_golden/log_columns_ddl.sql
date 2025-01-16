CREATE TABLE IF NOT EXISTS `table`
(
	`service_instance_id` LowCardinality(String) COMMENT 'service.instance.id',
	`service_name`        LowCardinality(String) COMMENT 'service.name',
	`service_namespace`   LowCardinality(String) COMMENT 'service.namespace',
	`timestamp`           DateTime64(9)          CODEC(Delta, ZSTD(1)),
	`severity_number`     UInt8,
	`severity_text`       LowCardinality(String),
	`trace_id`            FixedString(16),
	`span_id`             FixedString(8),
	`trace_flags`         UInt8,
	`body`                String,
	-- attribute attributes
	`attribute`           String,
	-- end
	-- resource attributes
	`resource`            LowCardinality(String),
	-- end
	`scope_name`          LowCardinality(String),
	`scope_version`       LowCardinality(String),
	-- scope attributes
	`scope`               LowCardinality(String),
	-- end

	INDEX `idx_trace_id`   trace_id TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX `idx_body`       body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
	INDEX `idx_ts`         timestamp TYPE minmax GRANULARITY 8192,
	INDEX `attribute_keys` arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(scope), JSONExtractKeys(resource)) TYPE set(100)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (`severity_number`, `service_namespace`, `service_name`, `resource`, `timestamp`)
PRIMARY KEY (`severity_number`, `service_namespace`, `service_name`, `resource`)
