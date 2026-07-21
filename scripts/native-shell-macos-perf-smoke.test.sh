#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HARNESS="${ROOT}/scripts/native-shell-macos-perf-smoke.sh"
bash -n "${HARNESS}"

for required in \
  'RSS_CEILING_KIB=204800' \
  'native-shell-perf-evaluate.py' \
  'sample_processes' \
  'stop_exact_pid' \
  'discover_daemon_pid' \
  'SAGE_CMT_RPC_ADDR=' \
  'process-samples.txt'; do
  if ! grep -Fq "${required}" "${HARNESS}"; then
    # A bare `grep -Fq` under `set -e` aborts with no output, which makes a
    # contract failure look like a crash. Say which invariant broke.
    echo "performance harness is missing a required invariant: ${required}" >&2
    exit 1
  fi
done

# 200 MiB exactly. This is the ceiling the framework decision rests on: the ADR
# rejected Electron at 358,720 KiB against it and selected Tauri at 142,544 KiB.
# A silently loosened ceiling would void that decision without anyone noticing.
python3 - "${HARNESS}" <<'PY'
import re, sys
src = open(sys.argv[1]).read()
m = re.search(r'^RSS_CEILING_KIB=(\d+)', src, re.M)
assert m, "the RSS ceiling is not declared as a literal"
assert int(m.group(1)) == 200 * 1024, f"ceiling is {m.group(1)} KiB, expected 204800 (200 MiB)"
PY

# Same containment rule as the other harnesses: exact executable paths only.
if grep -Eq 'pkill|killall|/proc/' "${HARNESS}"; then
  echo 'performance harness contains broad process-name cleanup' >&2
  exit 1
fi

if grep -Eq '(^|[^[:alnum:]_])(mapfile|readarray)([^[:alnum:]_]|$)' "${HARNESS}"; then
  echo 'performance harness uses bash 4 builtins unavailable on the system bash' >&2
  exit 1
fi

if ! grep -Eq 'SMOKE_ROOT=\$\(cd "\$\(mktemp -d.*\)" && pwd -P\)' "${HARNESS}"; then
  echo 'performance harness does not canonicalize its smoke root' >&2
  exit 1
fi

# Sampling must start only AFTER the app is renderable: this is a settled-state
# budget, not a startup transient, and sampling through boot would measure peak
# allocation instead.
python3 - "${HARNESS}" <<'PY'
import sys
src = open(sys.argv[1]).read()
ready = src.index('STATUS=$(query_status)')
sampler = src.index('sample_processes &')
assert ready < sampler, "sampling must start after the daemon reports a renderable state"
PY

fixture=$(mktemp -d "${TMPDIR:-/tmp}/sage-perf-fixture.XXXXXX")
trap 'rm -rf "${fixture}"' EXIT INT TERM
if "${HARNESS}" "${fixture}" 11.11.0-contract >"${fixture}/stdout" 2>"${fixture}/stderr"; then
  echo "performance harness unexpectedly accepted an empty bundle" >&2
  exit 1
fi
if [ "$(uname -s)" = "Darwin" ]; then
  grep -Fq 'expected exactly one staged macOS app' "${fixture}/stderr"
else
  grep -Fq 'requires macOS' "${fixture}/stderr"
fi

echo "native-shell macOS performance harness contract tests passed"
