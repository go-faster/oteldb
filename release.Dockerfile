ARG IMG=gcr.io/distroless/static-debian11
FROM $IMG:nonroot

COPY oteldb /usr/bin/local/oteldb

ENTRYPOINT ["/usr/bin/local/oteldb"]
