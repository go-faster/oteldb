#!/bin/bash

set -e -x

go install github.com/go-faster/oteldb/internal/promcompliance/cmd/promql-compliance-tester

docker compose up -d --remove-orphans --build --force-recreate
go run ./cmd/compliance-wait

echo ">> Testing oteldb implementation"
promql-compliance-tester -config-file promql-test-queries.yml -config-file test-oteldb.yml | tee result.oteldb.txt || true

docker compose down -v
