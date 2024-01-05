# promrw

## Recording

Start listener:
```bash
go run ./cmd/promrw -f /tmp/remotewrite.gob.zstd --listen -d 10m --addr="http://127.0.0.1:8080"
```

Start load generator:
```bash
go run ./cmd/prombench --targetsCount=100 --scrapeInterval=1s http://127.0.0.1:8080
```

Prometheus remote write requests will be recorded to `/tmp/remotewrite.gob.zstd` file.

## Replay

Use `ch-bench-read` docker compose and run:

```bash
go run ./cmd/promrw -f /tmp/remotewrite.gob.zstd --addr="http://127.0.0.1:19291"
```
