#!/usr/bin/env bash
set -euo pipefail

# wait_healthy <service> <label> <max_attempts> <sleep_secs>
# Polls the Docker health status of a compose service rather than re-running
# the probe command, so there are no TTY or PATH issues with exec.
wait_healthy() {
  local service=$1 label=$2 max=$3 interval=$4
  local attempt=0
  while true; do
    local id status
    id=$(docker-compose ps -q "$service" 2>/dev/null | head -1)
    status=$(docker inspect --format '{{.State.Health.Status}}' "$id" 2>/dev/null)
    if [ "$status" = "healthy" ]; then
      echo "  ${label} ready."
      return 0
    fi
    attempt=$((attempt + 1))
    if [ "$attempt" -ge "$max" ]; then
      echo "ERROR: ${label} did not become healthy after $((max * interval))s (last status: ${status:-unknown}). Aborting." >&2
      exit 1
    fi
    echo "  ${label} not ready (attempt ${attempt}/${max}, status: ${status:-unknown}), retrying in ${interval}s..."
    sleep "$interval"
  done
}

echo "Building image..."
docker-compose build

echo "Starting infrastructure..."
docker-compose up -d kafka minio prometheus grafana

echo "Waiting for Kafka..."
wait_healthy kafka "Kafka" 60 2

echo "Waiting for MinIO..."
wait_healthy minio "MinIO" 30 2

echo "Running init containers..."
docker-compose up kafka-init minio-init

echo "Starting producer (1) and consumers (4)..."
docker-compose up -d --scale consumer=4 producer consumer

echo ""
echo "All services running."
echo "  Grafana:    http://localhost:3000  (admin / admin)"
echo "  MinIO:      http://localhost:9001  (minioadmin / minioadmin)"
echo "  Prometheus: http://localhost:9090"
echo ""
echo "Run queries:  make query ARGS='top-paths --limit 10'"
