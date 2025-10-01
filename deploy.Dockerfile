FROM clickhouse/clickhouse-server

ADD oteldb /usr/local/bin/oteldb

ENTRYPOINT ["oteldb"]
