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

To start:

```
docker compose up -d
```

To collect profiles:
```
./prof.sh
```

This will emit profiles as `cpu.out` and `mem.out`:

```
go tool pprof -alloc_space mem.out
go tool pprof cpu.out
```