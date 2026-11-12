#!/usr/bin/env bash
# Start LanzyProxy: the WARP node pool plus the rotator in front of it.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

export WARP_ACCEPT_TOS="${WARP_ACCEPT_TOS:-1}"

echo ">> starting the WARP node pool"
docker compose up -d --build

echo ">> waiting for the nodes to register"
for i in $(seq 1 30); do
  ready=$(docker compose ps --format '{{.Name}} {{.Status}}' 2>/dev/null | grep -c 'healthy' || true)
  [ "$ready" -ge 12 ] && break
  sleep 5
done

UPSTREAMS=""
for port in 18090 18091 18092 18093 18094 18095; do
  UPSTREAMS="${UPSTREAMS}${UPSTREAMS:+,}127.0.0.1:${port}"
done

echo ">> starting the rotator on ${UPSTREAMS}"
exec python3 lanzyproxy.py \
  --upstreams "$UPSTREAMS" \
  --socks-port "${SOCKS_PORT:-1080}" \
  --http-port "${HTTP_PORT:-8082}" \
  --dashboard-port "${DASHBOARD_PORT:-8083}"
