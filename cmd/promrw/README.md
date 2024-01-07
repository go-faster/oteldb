# promrw

## Recording

Start listener:
```bash
go run ./cmd/promrw record -o /tmp/requests.rwq --d 10m --addr="127.0.0.1:8080"
```

Start load generator:
```bash
go run ./cmd/promrw bench --targetsCount=100 --scrapeInterval=1s http://127.0.0.1:8080
```

Prometheus remote write requests will be recorded to `/tmp/requests.rwq` file.

## Replay

Use `ch-bench-read` docker compose and run:

```bash
go run ./cmd/promrw send -i /tmp/remotewrite.gob.zstd -j 8 --target="http://127.0.0.1:19291"
```
