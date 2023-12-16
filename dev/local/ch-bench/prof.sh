#!/bin/bash

set -ex

# CPU
curl -o cpu.out http://localhost:9010/debug/pprof/profile?seconds=10

# Memory
curl -o mem.out http://localhost:9010/debug/pprof/heap?seconds=10
