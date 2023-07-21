<p align="center">
<img height="256" src="logo.svg" alt="oteldb svg logo">
</p>

# oteldb [![codecov](https://img.shields.io/codecov/c/github/go-faster/oteldb?label=cover)](https://codecov.io/gh/go-faster/oteldb) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

The OpenTelemetry-compatible telemetry aggregation, storage and processing.

Work in progress.

## Storage

The oteldb is stateless and uses external storage systems for data persistence, processing and aggregation.

### YTsaurus

The [YTsaurus](https://ytsaurus.tech/) is a primary storage for telemetry data.
An open source big data platform for distributed storage and processing.

- Hierarchical multi-tenancy with secure resource isolation
- OLAP and OLTP
- MapReduce, ACID
- ClickHouse protocol compatible
- Exabyte scale, up to 1M CPU, 10K+ nodes

### ClickHouse

The oteldb also supports [ClickHouse](https://clickhouse.com/) storage.

## Query

Supported query languages:
- LogQL (loki) for logs
- TraceQL (Tempo) for traces

## Local development

Setups Grafana, oteldb, storage and trace generator.

#### YTSaurus storage

```shell
docker compose -f dev/local/ytsaurus/docker-compose.yml up -d
```

#### Clickhouse storage

```shell
docker compose -f dev/local/clickhouse/docker-compose.yml up -d
```
