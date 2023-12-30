<p align="center">
<img height="256" src="logo.svg" alt="oteldb svg logo">
</p>

# oteldb [![codecov](https://img.shields.io/codecov/c/github/go-faster/oteldb?label=cover)](https://codecov.io/gh/go-faster/oteldb) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

The next generation, [OpenTelemetry][otel]-first signal aggregation system for metrics, traces and logs.

Compatible with [PromQL][promql], [TraceQL][traceql] and [LogQL][logql].

Based on [ClickHouse][clickhouse], fastest open-source column-oriented database.

[clickhouse]: https://clickhouse.com/
[otel]: https://opentelemetry.io/

> [!WARNING]
> Work in progress.

Supported query languages:
- [PromQL][promql] ([Prometheus][prometheus]) for metrics, [>99% compatibility][compliance]
- [TraceQL][traceql] ([Grafana Tempo][tempo]) for traces
- [LogQL][logql] ([Grafana Loki][loki]) for logs

[traceql]: https://grafana.com/docs/tempo/latest/traceql/
[logql]: https://grafana.com/docs/loki/latest/query/
[promql]: https://prometheus.io/docs/prometheus/latest/querying/basics/

[prometheus]: https://prometheus.io/
[loki]: https://grafana.com/oss/loki/
[tempo]: https://grafana.com/oss/tempo/

Supported ingestion protocols:
- Prometheus remote write, including [exemplars][exemplars]
- OpenTelemetry protocol (gRPC) for metrics, traces and logs

Ingestion is possible with [OpenTelemetry collector][otelcol], supporting [over 90 protocols][otelcol-contrib].

[otelcol]: https://opentelemetry.io/docs/collector/
[otelcol-contrib]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter
[exemplars]: https://grafana.com/docs/grafana/latest/fundamentals/exemplars/

## Prometheus Compatibility

See [ch-compliance][compliance] for Prometheus compatibility tests.

```console
$ promql-compliance-tester -config-file promql-test-queries.yml -config-file test.oteldb.yml
Total: 547 / 548 (99.82%) passed, 0 unsupported
```

[compliance]: ./dev/local/ch-compliance

## Quick Start

Setup oteldb, ClickHouse, Grafana, and telemetry generators:

```shell
docker compose -f dev/local/ch/docker-compose.yml up -d
```

You can open Grafana dashboard at http://localhost:3000/d/oteldb/oteldb

## License

Apache License 2.0, see [LICENSE](./LICENSE).
