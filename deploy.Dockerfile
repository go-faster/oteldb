FROM gcr.io/distroless/static

ADD bot /usr/local/bin/oteldb

ENTRYPOINT ["oteldb"]
