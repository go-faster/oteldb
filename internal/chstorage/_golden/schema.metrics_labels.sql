CREATE TABLE IF NOT EXISTS `metrics_labels`
(
	`name`             LowCardinality(String) COMMENT 'original name, i.e. foo.bar',
	`name_normalized`  LowCardinality(String) COMMENT 'normalized name, foo_bar',
	`value`            String,
	`value_normalized` String,
	`scope`            Enum8('NONE' = 0, 'RESOURCE' = 1, 'INSTRUMENTATION' = 2, 'ATTRIBUTE' = 4)
)
ENGINE = ReplacingMergeTree
ORDER BY (`name_normalized`, `value`, `scope`)
