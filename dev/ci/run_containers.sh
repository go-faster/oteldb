#!/usr/bin/env bash

set -e

docker run -d -p8000:80 ytsaurus/local:stable \
            --fqdn "localhost" \
            --proxy-config "{address_resolver={enable_ipv4=%true;enable_ipv6=%false;};coordinator={public_fqdn=\"localhost:8000\"}}" \
            --rpc-proxy-count "0" \
            --rpc-proxy-port "8002"

docker run -d -p8123:8123 -p9000:9000 -p9009:9009 clickhouse/clickhouse-server
