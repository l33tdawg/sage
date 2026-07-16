#!/usr/bin/env bash
# Fast, deterministic app-v20 multi-process fault oracle. The real CometBFT
# topology gate is deploy/scripts/run-v11.9-chaos.sh.

set -euo pipefail
cd "$(dirname "$0")/../.."

case "${V119_REQUIRE_SCOPED_RECONFIG:-0}" in
  0|1) ;;
  *)
    echo "ERROR: V119_REQUIRE_SCOPED_RECONFIG must be 0 or 1" >&2
    exit 1
    ;;
esac
case "${V119_MULTIPROCESS_RACE:-1}" in
  0|1) ;;
  *)
    echo "ERROR: V119_MULTIPROCESS_RACE must be 0 or 1" >&2
    exit 1
    ;;
esac

run_multiprocess_oracle() {
  go test "$@" ./internal/abci \
    -tags=multiprocess \
    -run '^TestV119(SignedScopeReconfigurationFaultHarness|DelegatedGovernanceReconfigurationCrashReplay)$' \
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

if [ "${V119_REQUIRE_SCOPED_RECONFIG:-0}" = "1" ]; then
  echo "PASS: required signed app-v20 scope formation/revision, held-replica catch-up, and pinned old/new ballots"
fi
