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
	-- attribute attributes
	`attribute`           LowCardinality(String),
	-- end
	-- resource attributes
	`resource`            LowCardinality(String),
	-- end
	-- scope attributes
	`scope`               LowCardinality(String),
	-- end
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (`name_normalized`, `resource`, `attribute`, `timestamp`)
