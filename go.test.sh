#!/usr/bin/env bash

set -e

# test with -race
go test --timeout 10m -race ./...
