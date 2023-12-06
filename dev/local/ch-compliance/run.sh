#!/bin/bash

set -e -x

cd ./compliance/promql && go install ./cmd/promql-compliance-tester && cd -

docker compose up -d --remove-orphans --build --force-recreate
go run ./cmd/compliance-wait

echo ">> Testing oteldb implementation"
promql-compliance-tester -config-file promql-test-queries.yml -config-file test.oteldb.yml | tee result.oteldb.txt || true

docker compose down -v
