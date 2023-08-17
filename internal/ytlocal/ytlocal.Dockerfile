ARG IMG=ubuntu:focal
FROM $IMG

USER root

# Main binary.
COPY ./ytserver-all /usr/bin/ytserver-all

# CHYT bits and pieces.
COPY ./chyt-controller /usr/bin/chyt-controller
COPY ./clickhouse-trampoline.py /usr/bin/clickhouse-trampoline
COPY ./ytserver-clickhouse /usr/bin/ytserver-clickhouse
COPY ./ytserver-log-tailer /usr/bin/ytserver-log-tailer

# Copy ytlocal
COPY ./ytlocal /usr/bin/ytlocal

ENTRYPOINT [ "/usr/bin/ytlocal" ]
