#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HARNESS="${ROOT}/scripts/native-shell-macos-offline-smoke.sh"
bash -n "${HARNESS}"

for required in \
  'sample_sockets' \
  'lsof' \
  'HTTPS_PROXY' \
  'ALL_PROXY' \
  'SSCP daemon is not renderable offline' \
  'non-loopback socket' \
  'stop_exact_pid' \
  'discover_daemon_pid' \
  'socket-samples.txt'; do
  grep -Fq "${required}" "${HARNESS}"
done

# The sampler must run continuously, not once: a single end-state check cannot
# see a transient boot-time request.
if ! grep -q 'while :; do' "${HARNESS}"; then
  echo 'offline harness does not sample sockets continuously' >&2
  exit 1
fi

# A settle window after ready is required so a delayed phone-home is still seen.
if ! grep -Eq 'sleep 5' "${HARNESS}"; then
  echo 'offline harness does not settle after reaching ready' >&2
  exit 1
fi

# Same containment rule as the lifecycle harness: exact executable paths only.
if grep -Eq 'pkill|killall|/proc/' "${HARNESS}"; then
  echo 'offline harness contains broad process-name cleanup' >&2
  exit 1
fi

if grep -Eq '(^|[^[:alnum:]_])(mapfile|readarray)([^[:alnum:]_]|$)' "${HARNESS}"; then
  echo 'offline harness uses bash 4 builtins unavailable on the system bash' >&2
  exit 1
fi

if ! grep -Eq 'SMOKE_ROOT=\$\(cd "\$\(mktemp -d.*\)" && pwd -P\)' "${HARNESS}"; then
  echo 'offline harness does not canonicalize its smoke root' >&2
  exit 1
fi

fixture=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-offline-fixture.XXXXXX")
trap 'rm -rf "${fixture}"' EXIT INT TERM
if "${HARNESS}" "${fixture}" 11.11.0-contract >"${fixture}/stdout" 2>"${fixture}/stderr"; then
  echo "offline harness unexpectedly accepted an empty bundle" >&2
  exit 1
fi
if [ "$(uname -s)" = "Darwin" ]; then
  grep -Fq 'expected exactly one staged macOS app' "${fixture}/stderr"
else
  grep -Fq 'requires macOS' "${fixture}/stderr"
fi

echo "native-shell macOS offline harness contract tests passed"
