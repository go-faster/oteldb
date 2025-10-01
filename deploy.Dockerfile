FROM clickhouse/clickhouse-server

ADD oteldb /usr/local/bin/oteldb
VOLUME /clickhouse

VOLUME /clickhouse
ENV EMBEDDED_CLICKHOUSE_HOST=0.0.0.0

ENTRYPOINT ["oteldb"]
