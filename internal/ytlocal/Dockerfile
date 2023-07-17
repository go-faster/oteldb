FROM ubuntu:20.04

RUN apt-get update && apt-get install -y ca-certificates curl wget git unzip gnupg2 lsb-release build-essential

# REFERENCE:
#  https://github.com/ytsaurus/ytsaurus/blob/main/BUILD.md

RUN wget -qO - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN wget -qO - https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
RUN wget -qO - https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - | apt-key add -
RUN echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-14 main" | tee /etc/apt/sources.list.d/llvm.list >/dev/null
RUN echo "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/kitware.list >/dev/null

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get install software-properties-common -y
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get update

RUN apt-get install -y python3-pip ninja-build libidn11-dev m4 clang-14 lld-14 cmake unzip
RUN python3 -m pip install PyYAML==6.0 conan==1.57.0 dacite

RUN wget -O protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v3.20.1/protoc-3.20.1-linux-x86_64.zip
RUN unzip protoc.zip -d /usr/local
RUN rm protoc.zip

RUN mkdir /build
WORKDIR /build
