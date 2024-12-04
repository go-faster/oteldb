CREATE TABLE IF NOT EXISTS `metrics_exemplars`
(
	`name`                LowCardinality(String),
	`name_normalized`     LowCardinality(String),
	`timestamp`           DateTime64(9)          CODEC(Delta, ZSTD(1)),
	`filtered_attributes` String,
	`exemplar_timestamp`  DateTime64(9)          CODEC(Delta, ZSTD(1)),
	`value`               Float64,
	`trace_id`            FixedString(16),
	`span_id`             FixedString(8),
	`attribute`           LowCardinality(String),
	`resource`            LowCardinality(String),
	`scope`               LowCardinality(String),

	INDEX `idx_arr_join_attribute` arrayJoin(JSONExtractKeys(attribute)) TYPE set(100),
	INDEX `idx_keys_attribute`     JSONExtractKeys(attribute) TYPE set(100),
	INDEX `idx_arr_join_resource`  arrayJoin(JSONExtractKeys(resource)) TYPE set(100),
	INDEX `idx_keys_resource`      JSONExtractKeys(resource) TYPE set(100),
	INDEX `idx_arr_join_scope`     arrayJoin(JSONExtractKeys(scope)) TYPE set(100),
	INDEX `idx_keys_scope`         JSONExtractKeys(scope) TYPE set(100)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (`name_normalized`, `resource`, `attribute`, `timestamp`)
