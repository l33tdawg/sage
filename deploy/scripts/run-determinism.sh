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
#
# For the app-v29 ladder run (DET_TEST=TestAppHashDeterminism_AppV29Activation):
#   DET_BLOCK_TIME=300ms  devnet block time (default 3s). The 200-block
#     consensus activation floor is NOT overridable — it is a governance rule —
#     but block time is a devnet knob, and the ladder walks app-v2..app-v28 one
#     rung at a time, so at the default 3s the run is ~4.5h and at 300ms ~45min.
#     Every validator carries the same value, so AppHash agreement is unaffected.
#   DET_TIMEOUT=5400s     go test ceiling (default 1800s); the ladder needs more.
#   DET_MIXED_ARCH=1      run validators 0/2 as linux/arm64 and 1/3 as
#     linux/amd64. This is the mixed-architecture quorum rehearsal app-v29
#     requires; Docker must support both through native execution or emulation.

set -euo pipefail
cd "$(dirname "$0")/../.."

PROJECT=sage-det
COMPOSE=(docker compose -p "${PROJECT}"
  -f deploy/docker-compose.yml
  -f deploy/docker-compose.test.yml
  -f deploy/docker-compose.det.yml)
if [ "${DET_MIXED_ARCH:-0}" = "1" ]; then
  COMPOSE+=(-f deploy/docker-compose.det-mixed.yml)
fi

cleanup() {
  echo "--- tearing down ${PROJECT} ---"
  # The password is required at interpolation time even for `down`; without it
  # the teardown aborts and every container and volume of the run is left
  # behind. It is the same throwaway value `up` used, never a real secret.
  POSTGRES_PASSWORD=ci_test_password "${COMPOSE[@]}" down -v --remove-orphans || true
}
trap cleanup EXIT

echo "--- regenerating a fresh 4-node testnet genesis (app_version 0) ---"
# SAGE_TESTNET_TIMEOUT_COMMIT is the devnet's block time knob (init-testnet.sh).
# Default 3s keeps every existing harness byte-identical.
export SAGE_TESTNET_TIMEOUT_COMMIT="${DET_BLOCK_TIME:-3s}"
echo "--- devnet block time: ${SAGE_TESTNET_TIMEOUT_COMMIT} ---"
bash deploy/init-testnet.sh

# Build the two shared images ONCE (docker-compose.det.yml resets the per-service
# build blocks — 8 independent cold builds exhausted Docker's VM disk). Sequential
# on purpose: one builder cache peak at a time. Skipped when the tag already
# exists; set DET_REBUILD=1 after code changes so the run tests the CURRENT tree.
build_det_image() {
  tag=$1
  dockerfile=$2
  platform=${3:-}
  if [ "${DET_REBUILD:-0}" != "1" ] && docker image inspect "${tag}" >/dev/null 2>&1; then
    created=$(docker image inspect --format '{{.Created}}' "${tag}")
    echo "!!! ${tag} already exists (built ${created}) — REUSING a possibly stale"
    echo "!!! binary. Set DET_REBUILD=1 to rebuild from the current tree."
    return
  fi
  echo "--- building ${tag} (${dockerfile}${platform:+, ${platform}}) ---"
  if [ -n "${platform}" ]; then
    docker build --platform "${platform}" -f "${dockerfile}" -t "${tag}" .
  else
    docker build -f "${dockerfile}" -t "${tag}" .
  fi
}

if [ "${DET_MIXED_ARCH:-0}" = "1" ]; then
  echo "--- mixed architecture: node0/node2 arm64, node1/node3 amd64 ---"
  for arch in arm64 amd64; do
    build_det_image "sage-det-abci:local-${arch}" deploy/Dockerfile.abci "linux/${arch}"
    build_det_image "sage-det-node:local-${arch}" deploy/Dockerfile.node "linux/${arch}"
  done
else
  build_det_image "sage-det-abci:local" deploy/Dockerfile.abci
  build_det_image "sage-det-node:local" deploy/Dockerfile.node
fi

echo "--- starting the isolated cluster ---"
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

if [ "${DET_MIXED_ARCH:-0}" = "1" ]; then
  echo "--- verifying live validator architectures ---"
  for spec in "abci0 aarch64" "abci1 x86_64" "abci2 aarch64" "abci3 x86_64" \
              "cometbft0 aarch64" "cometbft1 x86_64" \
              "cometbft2 aarch64" "cometbft3 x86_64"; do
    service=${spec%% *}
    want=${spec##* }
    got=$(docker exec "${PROJECT}-${service}-1" uname -m)
    if [ "${got}" != "${want}" ]; then
      echo "ERROR: ${service} is running ${got}, want ${want}" >&2
      exit 1
    fi
    echo "${service}: ${got}"
  done
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
# DET_TEST overrides which determinism test runs (default: the general 4-node gate).
# e.g. DET_TEST=TestAppHashDeterminism_AppV11Activation to exercise the app-v11 seam.
DET_TEST="${DET_TEST:-TestAppHashDeterminism_FourValidators}"
echo "--- test: ${DET_TEST} ---"
SAGE_TEST_API_URL=http://localhost:18090 \
SAGE_TEST_API0=http://localhost:18090 SAGE_TEST_API1=http://localhost:18091 \
SAGE_TEST_API2=http://localhost:18092 SAGE_TEST_API3=http://localhost:18093 \
SAGE_TEST_RPC0=http://localhost:36657 SAGE_TEST_RPC1=http://localhost:36757 \
SAGE_TEST_RPC2=http://localhost:36857 SAGE_TEST_RPC3=http://localhost:36957 \
go test ./test/integration/ -run "${DET_TEST}" \
  -tags=integration -count=1 -v -timeout "${DET_TIMEOUT:-1800s}" $SHORT_FLAG

echo "=== DETERMINISM RUN PASSED: AppHash byte-identical across all 4 nodes ==="
