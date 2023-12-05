package chstorage

const schemaMigration = `
CREATE TABLE IF NOT EXISTS %s (
	table  String,
	ddl    String,
	ts 	   DateTime DEFAULT now(),
) ENGINE = ReplacingMergeTree(ts) 
ORDER BY (table, ddl);`
