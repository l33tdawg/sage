#!/usr/bin/env bash
#
# Cross-node AppHash determinism harness (audit residual #6).
#
# Stands up an ISOLATED 4-validator devnet (compose project `sage-det`, REST ports
# 18090-18093, RPC 36657-36957), then runs TestAppHashDeterminism_FourValidators,
# which asserts every node's committed AppHash is byte-identical at matched
# heights — across an epoch boundary and across a real fork activation. Tears the
# cluster down on exit.
#
# COST / SAFETY:
#   * Cold-builds the Go node image (Dockerfile.abci compiles from source) and
#     runs ~15-20 min (the 200-block activation floor is NOT env-overridable).
#     Run it only when the machine has spare CPU/RAM.
#   * `init-testnet.sh` regenerates deploy/genesis (a throwaway devnet dir). Do
#     NOT run while another process depends on deploy/genesis.
#   * Isolated under `-p sage-det`; does not touch other compose projects. It does
#     NOT run `make down-clean`.
#
# Set DET_SHORT=1 to skip the slow fork-activation phase (epoch-boundary phase only).

set -euo pipefail
cd "$(dirname "$0")/../.."

PROJECT=sage-det
COMPOSE=(docker compose -p "${PROJECT}"
  -f deploy/docker-compose.yml
  -f deploy/docker-compose.test.yml
  -f deploy/docker-compose.det.yml)

cleanup() {
  echo "--- tearing down ${PROJECT} ---"
  "${COMPOSE[@]}" down -v --remove-orphans || true
}
trap cleanup EXIT

echo "--- regenerating a fresh 4-node testnet genesis (app_version 0) ---"
bash deploy/init-testnet.sh

echo "--- building + starting the isolated cluster (cold build, be patient) ---"
# Only the services the determinism test needs: it submits NO memories, so the
# ollama embedding stack (which pulls ~1.3GB of models) is skipped — abci depends
# only on postgres, not ollama.
POSTGRES_PASSWORD=ci_test_password "${COMPOSE[@]}" up -d --build \
  postgres abci0 abci1 abci2 abci3 cometbft0 cometbft1 cometbft2 cometbft3

echo "--- waiting for all 4 REST endpoints to report healthy ---"
healthy=0
for _ in $(seq 1 150); do
  healthy=0
  for p in 18090 18091 18092 18093; do
    if curl -fsS "http://localhost:${p}/health" >/dev/null 2>&1; then
      healthy=$((healthy + 1))
    fi
  done
  if [ "${healthy}" -eq 4 ]; then
    echo "all 4 nodes healthy"
    break
  fi
  sleep 2
done
if [ "${healthy}" -ne 4 ]; then
  echo "ERROR: cluster did not become healthy in time"
  "${COMPOSE[@]}" ps
  exit 1
fi

# Plain string, not an array: macOS bash 3.2 + `set -u` errors on an empty
# array's "${arr[@]}" expansion. An unquoted empty string simply expands to no
# argument, which is exactly what we want here (a single optional flag).
SHORT_FLAG=
if [ "${DET_SHORT:-0}" = "1" ]; then
  SHORT_FLAG=-short
  echo "--- DET_SHORT=1: epoch-boundary phase only (skipping fork activation) ---"
fi

echo "--- running the determinism test ---"
SAGE_TEST_API_URL=http://localhost:18090 \
SAGE_TEST_API0=http://localhost:18090 SAGE_TEST_API1=http://localhost:18091 \
SAGE_TEST_API2=http://localhost:18092 SAGE_TEST_API3=http://localhost:18093 \
SAGE_TEST_RPC0=http://localhost:36657 SAGE_TEST_RPC1=http://localhost:36757 \
SAGE_TEST_RPC2=http://localhost:36857 SAGE_TEST_RPC3=http://localhost:36957 \
  go test ./test/integration/ -run TestAppHashDeterminism_FourValidators \
  -tags=integration -count=1 -v -timeout 1800s $SHORT_FLAG

echo "=== DETERMINISM RUN PASSED: AppHash byte-identical across all 4 nodes ==="
