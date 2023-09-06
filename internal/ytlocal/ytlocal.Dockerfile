ARG IMG=ubuntu:focal
FROM $IMG

USER root

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
    curl \
    less \
    gdb \
    lsof \
    strace \
    telnet \
    dnsutils \
    iputils-ping \
    lsb-release \
    python3.7 \
    python3-pip \
    python3.7-distutils
RUN ln -s /usr/bin/python3.7 /usr/bin/python3 -f

RUN python3.7 -m pip install ytsaurus-client ytsaurus-yson

# Main binary.
COPY ./ytserver-all /usr/bin/ytserver-all
RUN ln -s /usr/bin/ytserver-all /usr/bin/ytserver-job-proxy

# CHYT bits and pieces.
COPY ./chyt-controller /usr/bin/chyt-controller
COPY ./clickhouse-trampoline.py /usr/bin/clickhouse-trampoline
COPY ./ytserver-clickhouse /usr/bin/ytserver-clickhouse
COPY ./ytserver-log-tailer /usr/bin/ytserver-log-tailer

# Copy ytlocal
COPY ./ytlocal /usr/bin/ytlocal

ENTRYPOINT [ "/usr/bin/ytlocal" ]
