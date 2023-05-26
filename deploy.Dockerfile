FROM gcr.io/distroless/static

ADD oteldb /usr/local/bin/oteldb

ENTRYPOINT ["oteldb"]
