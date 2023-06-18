# oteldb [![codecov](https://img.shields.io/codecov/c/github/go-faster/oteldb?label=cover)](https://codecov.io/gh/go-faster/oteldb) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

Work in progress.

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
