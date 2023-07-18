# ytlocal

This package provides a way to launch a local version
of ytsaurus cluster.



## Building

First, prepare environment:

```bash
./prepare.sh
```

This will build the `yt-builder` image.

To reuse prebuild image, skip this step:
```bash
cd _ytwork

git clone https://github.com/ytsaurus/ytsaurus.git
mkdir -p build
```

Then, build the package:

```bash
./build.sh
```

This will build the `ytserver-all` binary with `ninja`.

Resulting binary will be at `/_ytwork/build/yt/yt/server/all/ytserver-all`.
