#!/bin/bash

set -e -x

docker build -t ghcr.io/go-faster/yt-build .

cd _ytwork

git clone https://github.com/ytsaurus/ytsaurus.git
mkdir -p build
