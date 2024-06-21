#!/bin/bash

clean_up() {
    ARG=$?
    echo ">> Stopping"
    docker compose down -v
    exit $ARG
}
trap clean_up EXIT

set -e

echo ">> Setup oteldb locally"
docker compose up -d --remove-orphans --build --force-recreate

start_time="2024-01-01T00:01:10Z"
end_time="2024-01-01T00:05:10Z"

echo ">> Generate benchmark data"
go run github.com/go-faster/oteldb/cmd/otelbench otel logs bench \
    localhost:4318 \
    --seed 10 \
    --start "$start_time" \
    --rate 1ms \
    --entries 150 \
    --clickhouseAddr "localhost:9000" \
    --total 100000

echo ">> Benchmark"
queries_file="${BENCH_QUERIES:-testdata/logql.yml}"
benchmark_runs="${BENCH_RUNS:-15}"

OTEL_EXPORTER_OTLP_INSECURE="true" go run github.com/go-faster/oteldb/cmd/otelbench logql bench \
    --start "$start_time" \
    --end "$end_time" \
    -i "$queries_file" \
    -o report.yml \
    --trace \
    --allow-empty=false \
    --count "$benchmark_runs" \
    --warmup 5

echo ">> Done"
