FROM gcr.io/distroless/static

ADD otelproxy /usr/local/bin/otelproxy

ENTRYPOINT ["otelproxy"]
