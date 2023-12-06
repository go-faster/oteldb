#!/bin/bash

set -e -x

docker compose up -d --remove-orphans --build --force-recreate

go run ./cmd/compliance-wait -wait 10s

echo ">> Testing oteldb implementation"
RANGE="1m"
END="1m"
go run github.com/go-faster/oteldb/cmd/promql-compliance-tester \
  -end "${END}" -range "${RANGE}" \
  -config-file promql-test-queries.yml -config-file test-oteldb.yml | tee result.oteldb.txt || true

docker compose down -v
