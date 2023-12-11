FROM gcr.io/distroless/static

ADD oteldemo /usr/local/bin/oteldemo

ENTRYPOINT ["oteldemo"]
