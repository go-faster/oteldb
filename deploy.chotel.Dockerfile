FROM gcr.io/distroless/static

ADD chotel /usr/local/bin/chotel

ENTRYPOINT ["chotel"]
