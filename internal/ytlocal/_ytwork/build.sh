#!/bin/bash

set -e -x
conan --version

cd build
# cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../ytsaurus/clang.toolchain ../ytsaurus

ninja
