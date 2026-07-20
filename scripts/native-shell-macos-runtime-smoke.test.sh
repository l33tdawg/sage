#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HARNESS="${ROOT}/scripts/native-shell-macos-runtime-smoke.sh"
bash -n "${HARNESS}"

for required in \
  'hdiutil attach' \
  'hdiutil detach' \
  'LOCAL_PEERPID' \
  'ditto' \
  'SAGE_CMT_RPC_ADDR=' \
  'SAGE_CMT_P2P_ADDR=' \
  'SSCP status schema is not exact' \
  'SSCP daemon is not renderable' \
  'SSCP startup proof is missing or malformed' \
  'SSCP peer pid is not derivable' \
  'shell-control.sock' \
  "stat -f '%A'" \
  'instance_generation' \
  'require_relaunch_exit' \
  'require_socket_gone' \
  'stop_exact_pid' \
  'discover_daemon_pid' \
  'refusing to signal pid' \
  'preserve.sentinel' \
  'Contents/Resources/binaries/sage-gui' \
  'served SSCP from an unexpected daemon' \
  'STAGED_GENERATION' \
  'REINSTALLED_GENERATION'; do
  grep -Fq "${required}" "${HARNESS}"
done

# Containment: the harness must never reach for a process by name, and must not
# fall back to the Linux-only /proc environment scan.
if grep -Eq 'pkill|killall|-o comm=[[:space:]]*\|[[:space:]]*grep|/proc/' "${HARNESS}"; then
  echo 'macOS runtime harness contains broad process-name cleanup' >&2
  exit 1
fi

# bash 3.2 is the system bash on an Apple runner; these builtins are not there.
if grep -Eq '(^|[^[:alnum:]_])(mapfile|readarray)([^[:alnum:]_]|$)' "${HARNESS}"; then
  echo 'macOS runtime harness uses bash 4 builtins unavailable on the system bash' >&2
  exit 1
fi

# The isolated install root must never be the real /Applications.
if grep -Eq 'INSTALL_ROOT=["'"'"']?/Applications' "${HARNESS}"; then
  echo 'macOS runtime harness installs into the shared /Applications root' >&2
  exit 1
fi

# Regression: mktemp reports the unresolved /var path while the kernel reports
# exec'd binaries under /private/var, so an uncanonicalized smoke root makes the
# daemon-identity comparison fail on every run.
if ! grep -Eq 'SMOKE_ROOT=\$\(cd "\$\(mktemp -d.*\)" && pwd -P\)' "${HARNESS}"; then
  echo 'macOS runtime harness does not canonicalize its smoke root' >&2
  exit 1
fi

fixture=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-macos-fixture.XXXXXX")
trap 'rm -rf "${fixture}"' EXIT INT TERM
if "${HARNESS}" "${fixture}" 11.11.0-contract >"${fixture}/stdout" 2>"${fixture}/stderr"; then
  echo "runtime harness unexpectedly accepted an empty bundle" >&2
  exit 1
fi
if [ "$(uname -s)" = "Darwin" ]; then
  grep -Fq 'expected exactly one DMG package' "${fixture}/stderr"
else
  grep -Fq 'requires macOS' "${fixture}/stderr"
fi

echo "native-shell macOS runtime harness contract tests passed"
