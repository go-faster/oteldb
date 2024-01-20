FROM gcr.io/distroless/static

ADD otelbot /usr/local/bin/otelbot

ENTRYPOINT ["otelbot"]
