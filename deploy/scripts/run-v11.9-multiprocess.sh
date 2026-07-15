#!/usr/bin/env bash
# Fast, deterministic app-v20 multi-process fault oracle. The real CometBFT
# topology gate is deploy/scripts/run-v11.9-chaos.sh.

set -euo pipefail
cd "$(dirname "$0")/../.."

run_multiprocess_oracle() {
  go test "$@" ./internal/abci \
    -tags=multiprocess \
    -run '^TestV119MultiProcessFaultHarness$' \
    -count=1 \
    -v \
    -timeout "${V119_MULTIPROCESS_TIMEOUT:-240s}"
}

# Branch instead of expanding an empty array: Bash 3.2 treats a declared empty
# array as unbound under `set -u`, which made the documented no-race mode fail.
if [ "${V119_MULTIPROCESS_RACE:-1}" = "1" ]; then
  run_multiprocess_oracle -race
else
  run_multiprocess_oracle
fi
