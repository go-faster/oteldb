<p align="center">
<img height="256" src="logo.svg" alt="oteldb svg logo">
</p>

# oteldb [![codecov](https://img.shields.io/codecov/c/github/go-faster/oteldb?label=cover)](https://codecov.io/gh/go-faster/oteldb) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

The OpenTelemetry-compatible telemetry aggregation, storage and processing.

Work in progress.

> [!WARNING]  
> Work in progress.

## Storage

The oteldb is stateless and uses external storage systems for data persistence, processing and aggregation.

We focus on the following storage systems:
- [ClickHouse](https://clickhouse.com/)
- [YTsaurus](https://ytsaurus.tech/)

Currently, ClickHouse looks more promising.

## Query

Supported query languages:
- LogQL (loki) for logs
- TraceQL (Tempo) for traces
- PromQL (Prometheus) for metrics

## Local development

Setups Grafana, oteldb, storage and trace generator.

#### YTSaurus storage

```shell
docker compose -f dev/local/ytsaurus/docker-compose.yml up -d
```

#### Clickhouse storage

```shell
docker compose -f dev/local/ch/docker-compose.yml up -d
```
