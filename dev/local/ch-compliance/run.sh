#!/bin/bash

clean_up () {
    ARG=$?
    echo ">> Stopping"
    docker compose down -v
    exit $ARG
}
trap clean_up EXIT

set -e

docker compose up -d --remove-orphans --build --force-recreate

go run ./cmd/compliance-wait -wait 10s

echo ">> Testing oteldb implementation"
RANGE="1m"
END="1m"
go run github.com/go-faster/oteldb/cmd/promql-compliance-tester \
  -end "${END}" -range "${RANGE}" \
  -config-file promql-test-queries.yml -config-file test-oteldb.yml -output-format json > result.oteldb.json || true

go run ./cmd/compliance-verify result.oteldb.json


