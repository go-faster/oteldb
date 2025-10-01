ARG IMG=clickhouse/clickhouse-server
FROM $IMG:nonroot

COPY oteldb /usr/bin/local/oteldb

VOLUME /clickhouse
ENV EMBEDDED_CLICKHOUSE_HOST=0.0.0.0

ENTRYPOINT ["/usr/bin/local/oteldb"]
