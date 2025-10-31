ARG IMG=gcr.io/distroless/static
FROM $IMG

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/oteldb /usr/bin/local/oteldb

VOLUME /clickhouse
ENV EMBEDDED_CLICKHOUSE_HOST=0.0.0.0

ENTRYPOINT ["/usr/bin/local/oteldb"]
