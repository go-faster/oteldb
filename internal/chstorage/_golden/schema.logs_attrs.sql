CREATE TABLE IF NOT EXISTS `logs_attrs`
(
	`name` String COMMENT 'foo_bar',
	`key`  String COMMENT 'foo.bar'
)
ENGINE = ReplacingMergeTree
ORDER BY (`name`)
