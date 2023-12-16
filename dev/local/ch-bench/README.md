# ch-bench

This docker compose project will run a [prometheus-benchmark](https://github.com/VictoriaMetrics/prometheus-benchmark)
composed as a single binary (see ./cmd/prombench) against single instance of oteldb.

The `probmench` runs `node_exporter` and `vmagent`, acting as a dynamic configuration source for `vmagent` scrape targets
and a caching proxy before `node_exporter`.

This generates load as a `vmagent` scrapes generated config and sends metrics to the configured remote write address per `scrapeInterval`.

Total generated load is controlled by following arguments:
- `-targetsCount`, number of targets to generate (i.e. "virtual" node exporter instances)
- `-scrapeInterval`, interval between scrapes of each target

So total metrics per second would be `(targetsCount * scrapedMetrics) / scrapeInterval`, where
`scrapedMetrics` is number of metrics scraped from node exporter.

The `scapedMetrics` depends on machine where node exporter runs, but it averages around 1400-1500.

The `scrapeConfigUpdateInterval` and `scrapeConfigUpdatePercent` controls new "tags" generation.
This does not impact metrics per second, but will generate more unique metrics.

## Attacking

```
docker compose up -d
```

## Profiling

To collect profiles:
```
./prof.sh
```

This will emit profiles as `cpu.out` and `mem.out`:

```
go tool pprof -alloc_space mem.out
go tool pprof cpu.out
```

## Checking

To check that points actually made to database, one can use following query:
```clickhouse
SELECT toDateTime(toStartOfSecond(timestamp)) as ts, COUNT()
FROM metrics_points
WHERE timestamp < (now() - toIntervalSecond(5))
GROUP BY ts
ORDER BY ts DESC
LIMIT 15;
```

Or see attacker logs, points_per_sec attribute:

```console
docker compose logs attacker --no-log-prefix | grep Reporting | tail -n5 | jq -c
```
```json
{"level":"info","ts":1702747705.4254293,"caller":"prombench/main.go:227","msg":"Reporting","hash":"9659488dfc5b1296","scraped.total":1437,"scraped.size":102828,"metrics.total":143700,"points_per_sec":144300}
```

```console
$ docker compose logs attacker --no-log-prefix | grep Reporting | tail -n5 | jq -r .points_per_sec
144400
144400
144400
144353
144353
```