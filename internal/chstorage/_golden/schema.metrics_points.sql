CREATE TABLE IF NOT EXISTS `metrics_points`
(
	`name`      LowCardinality(String) CODEC(ZSTD(1)),
	`timestamp` DateTime64(9)          CODEC(Delta, ZSTD(1)),
	`mapping`   Enum8(
		'NO_MAPPING' = 0,
		'HISTOGRAM_COUNT' = 1,
		'HISTOGRAM_SUM' = 2,
		'HISTOGRAM_MIN' = 3,
		'HISTOGRAM_MAX' = 4,
		'HISTOGRAM_BUCKET' = 5,
		'SUMMARY_COUNT' = 6,
		'SUMMARY_SUM' = 7,
		'SUMMARY_QUANTILE' = 8
		) CODEC(T64, ZSTD(1)),
	`value`     Float64                CODEC(Gorilla, ZSTD(1)),
	`flags`     UInt8                  CODEC(T64, ZSTD(1)),
	`attribute` LowCardinality(String),
	`resource`  LowCardinality(String),
	`scope`     LowCardinality(String),

	INDEX `idx_ts` timestamp TYPE minmax GRANULARITY 8192
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (`name`, `mapping`, `resource`, `attribute`, `timestamp`)
PRIMARY KEY (`name`, `mapping`, `resource`, `attribute`)
TTL toDateTime(`timestamp`) + toIntervalSecond(259200)