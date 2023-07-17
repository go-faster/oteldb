#!/bin/bash

# using current user and group for build folder to be accessible from host
# HACK: setting $HOME for conan because it wants to write to home directory

set -e -x

docker run -it --rm --init -e "HOME=/build" --ipc=host -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro --name yt-builder --user $(id -u):$(id -g) -v ./ytwork:/build ghcr.io/go-faster/yt-build ./build.sh
