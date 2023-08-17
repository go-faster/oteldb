#!/bin/bash

set -e -x

SRC_DIR="_ytwork/ytsaurus"
BUILD_DIR="_ytwork/build"

# Copy main binary.
cp "${BUILD_DIR}/yt/yt/server/all/ytserver-all" ./ytserver-all

# Build CHYT controller.
CHYT_CONTROLLER_DIR="${SRC_DIR}/yt/chyt/controller"
pushd "$CHYT_CONTROLLER_DIR"
    CGO_ENABLED=0 go build -o ./chyt-controller "./cmd/chyt-controller"
popd
cp "${CHYT_CONTROLLER_DIR}/chyt-controller" ./chyt-controller

# Copy Clickhouse engine.
cp "${BUILD_DIR}/yt/chyt/server/bin/ytserver-clickhouse" ./ytserver-clickhouse
cp "${SRC_DIR}/yt/chyt/trampoline/clickhouse-trampoline.py" ./clickhouse-trampoline.py
# Copy log tailer.
cp "${BUILD_DIR}/yt/yt/server/log_tailer/bin/ytserver-log-tailer" ./ytserver-log-tailer

# Build ytlocal.
CGO_ENABLED=0 go build -o ytlocal ../../cmd/ytlocal

# Build image.
docker build -f ytlocal.Dockerfile -t ghcr.io/go-faster/ytlocal .

# Cleanup.
rm ytserver-all \
    chyt-controller \
    ytserver-clickhouse \
    clickhouse-trampoline.py \
    ytserver-log-tailer \
    ytlocal
