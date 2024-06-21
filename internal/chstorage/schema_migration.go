package chstorage

const schemaMigration = `
(
	table  String,
	ddl    String,
	ts 	   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ts)
ORDER BY (table)`
