#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HARNESS="${ROOT}/scripts/native-shell-linux-runtime-smoke.sh"
bash -n "${HARNESS}"

for required in \
  'sudo dpkg -i' \
  'sudo dpkg --remove' \
  'APPIMAGE_EXTRACT_AND_RUN=1' \
  'shell-control.sock' \
  "stat -c '%a'" \
  'instance_generation' \
  'require_relaunch_exit' \
  'preserve.sentinel' \
  'matching_smoke_pids'; do
  grep -Fq "${required}" "${HARNESS}"
done

fixture=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-runtime-fixture.XXXXXX")
trap 'rm -rf "${fixture}"' EXIT INT TERM
if "${HARNESS}" "${fixture}" >"${fixture}/stdout" 2>"${fixture}/stderr"; then
  echo "runtime harness unexpectedly accepted an empty bundle" >&2
  exit 1
fi
if [ "$(uname -s)" = "Linux" ]; then
  grep -Fq 'expected exactly one Debian package' "${fixture}/stderr"
else
  grep -Fq 'requires Linux' "${fixture}/stderr"
fi

echo "native-shell Linux runtime harness contract tests passed"
