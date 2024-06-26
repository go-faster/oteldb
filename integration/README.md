# Integration tests

This directory contains Prometheus, Tempo and Loki API integration tests.

Docker is required to run.

### Getting traces from tests for debugging

Run a local [`Jaeger`](https://www.jaegertracing.io/) instance:

```console
docker run --rm -d --name jaeger -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest
```

Run wanted test with `E2E_TRACES_EXPORTER`:

```console
E2E_TRACES_EXPORTER='localhost:4317' E2E='1' go test -v -run "TestCH/QueryRange" ./integration/prome2e
```

Open Jaeger UI [http://localhost:16686/](http://localhost:16686/).
