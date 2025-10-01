FROM clickhouse/clickhouse-server

ADD oteldb /usr/local/bin/oteldb
VOLUME /clickhouse

ENTRYPOINT ["oteldb"]
