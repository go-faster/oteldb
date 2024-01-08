# otelbench

```
go install ./cmd/otelbench
```

## Prometheus Remote Write

### Benchmark

Requires `vmagent` and `node_exporter` binaries to be available in `$PATH`.

```console
Start remote write benchmark

Usage:
  otelbench promrw bench [flags]

Flags:
      --addr string                           address to listen (default "127.0.0.1:8428")
      --agentAddr string                      address for vmagent to listen (default "127.0.0.1:8429")
      --clickhouseAddr string                 clickhouse tcp protocol addr to get actual stats from
  -h, --help                                  help for bench
      --nodeExporterAddr string               address for node exporter to listen (default "127.0.0.1:9301")
      --pollExporterInterval duration         Interval to poll the node exporter filling up cache (default 1s)
      --queryAddr string                      addr to query PromQL from
      --queryInterval duration                interval to query PromQL (default 5s)
      --scrapeConfigUpdateInterval duration   The -scrapeConfigUpdatePercent scrape targets are updated in the scrape config returned from -httpListenAddr every -scrapeConfigUpdateInterval (default 10m0s)
      --scrapeConfigUpdatePercent float       The -scrapeConfigUpdatePercent scrape targets are updated in the scrape config returned from -httpListenAddr ever -scrapeConfigUpdateInterval (default 1)
      --scrapeInterval duration               The scrape_interval to set at the scrape config returned from -httpListenAddr (default 5s)
      --targetsCount int                      The number of scrape targets to return from -httpListenAddr. Each target has the same address defined by -targetAddr (default 100)
      --useVictoria                           use vmagent instead of prometheus (default true)
```

```bash
otelbench promrw bench http://127.0.0.1:19291
```

### Record

Start listener:
```bash
otelbench promrw record -o /tmp/requests.rwq --d 10m --addr="127.0.0.1:8080"
```

Start load generator:
```bash
otelbench promrw bench --targetsCount=10 --scrapeInterval=1s http://
```

Prometheus remote write requests will be recorded to `/tmp/requests.rwq` file.

### Replay

Start prometheus remote write endpoint, for example `ch-bench-read`:

```bash
docker compose up -f ./dev/local/ch-bench-read/docker-compose.yml -d
```

Replay prometheus remote write requests, sending them to specified target:
```bash
otelbench promrw replay -i /tmp/requests.rwq -j 8 --target="http://127.0.0.1:19291"
```

## PromQL

```bash
otelbench promql bench -i ./internal/promproxy/testdata/node-exporter.jsonl -o /tmp/report.yml
otelbench promql analyze -i /tmp/report.yml
```
