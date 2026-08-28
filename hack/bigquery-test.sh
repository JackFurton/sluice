#!/usr/bin/env bash
#
# Runs the BigQuery sink against the BigQuery emulator.
#
# The sink is the part of this operator that touches a real warehouse. Testing
# it against a fake would only prove the fake matches the code, so this runs it
# against something that speaks the actual API.

set -euo pipefail

IMAGE="${BQ_EMULATOR_IMAGE:-ghcr.io/goccy/bigquery-emulator:0.6.6}"
PORT="${BQ_EMULATOR_PORT:-9052}"
NAME="sluice-bq-emulator-$$"
PROJECT="test-project"
DATASET="vendor"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

cleanup() { docker rm -f "${NAME}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 1; }

echo "==> starting the BigQuery emulator"
# The emulator publishes no arm64 image, so the platform is pinned. On an
# Apple machine this runs under emulation, which is slower to start and fine
# for the handful of requests these tests make.
docker run -d --platform linux/amd64 --name "${NAME}" -p "${PORT}:9050" "${IMAGE}" \
  --project="${PROJECT}" --dataset="${DATASET}" >/dev/null

echo "==> waiting for it to answer"
ready=""
for _ in $(seq 1 60); do
  if curl -sf -o /dev/null "http://localhost:${PORT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}"; then
    ready=yes
    break
  fi
  sleep 2
done
if [ -z "${ready}" ]; then
  echo "the emulator never became ready" >&2
  docker logs "${NAME}" 2>&1 | tail -20
  exit 1
fi

echo "==> running the sink tests"
BIGQUERY_EMULATOR_HOST="http://localhost:${PORT}" \
  go test -tags integration -count=1 ./internal/worker/ -run TestBigQuery -v
