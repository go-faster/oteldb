FROM gcr.io/distroless/static

ADD otelfaker /usr/local/bin/otelfaker

ENTRYPOINT ["otelfaker"]
