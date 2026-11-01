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

# Run the real post-ready settle/cleanup section with harmless owned children.
# No app, daemon, socket, or GUI is launched. In particular, a ready daemon must
# not let a shell that exits during settle turn into a successful offline smoke.
python3 - "${HARNESS}" "${fixture}/liveness.sh" <<'PY'
import re
import sys
from pathlib import Path

source = Path(sys.argv[1]).read_text()
functions = []
for name in ("track_pid", "process_path", "stop_exact_pid", "launch_shell",
             "require_shell_running", "stop_shell"):
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}", source, re.M | re.S)
    if match:
        functions.append(match.group())
tail = source[source.index("# Settle: keep sampling after ready"):]
Path(sys.argv[2]).write_text('''#!/usr/bin/env bash
set -euo pipefail
''' + "\n\n".join(functions) + '''
scenario=$1
SAGE_SMOKE_HOME=$2
SHELL_LOG=${SAGE_SMOKE_HOME}/shell.log
SAMPLES=${SAGE_SMOKE_HOME}/socket-samples.txt
EXIT_MARKER=${SAGE_SMOKE_HOME}/exit
REST_PORT=0 RPC_PORT=0 P2P_PORT=0
TRACKED_PIDS=""
mkdir -p "${SAGE_SMOKE_HOME}"
: > "${SAMPLES}"
cleanup_children() {
  local pid
  for pid in ${TRACKED_PIDS}; do
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  done
}
trap cleanup_children EXIT
if [ "${scenario}" = alive ]; then
  STUB_EXECUTABLE=/bin/sleep
  launch_shell /bin/sleep 30
else
  STUB_EXECUTABLE=/bin/sh
  launch_shell /bin/sh -c '
    while [ ! -e "$1" ]; do sleep 0.01; done
    exit "$2"
  ' stub-shell "${EXIT_MARKER}" "${scenario##*-}"
fi
SHELL_PID=${LAUNCHED_PID}
# env must exec the native binary in place; a live launcher is insufficient.
SHELL_EXECUTABLE=${STUB_EXECUTABLE}
if [ "$(uname -s)" != Darwin ]; then
  SHELL_EXECUTABLE=${STUB_EXECUTABLE##*/}
fi
deadline=$((SECONDS + 5))
until [ "$(process_path "${SHELL_PID}")" = "${SHELL_EXECUTABLE}" ]; do
  test "${SECONDS}" -lt "${deadline}"
  command sleep 0.01
done
kill -0 "${SHELL_PID}"
/bin/sleep 30 &
SAMPLER_PID=$!
track_pid "${SAMPLER_PID}"
/bin/sleep 30 &
DAEMON_PID=$!
track_pid "${DAEMON_PID}"
sleep() {
  if [ "$1" = 5 ]; then
    case "${scenario}" in settle-*) : > "${EXIT_MARKER}" ;; esac
    command sleep 0.2
  else
    command sleep "$@"
  fi
}
case "${scenario}" in shutdown-*)
  # Control the exit after the last running check, before intentional TERM.
  stop_exact_pid() {
    if [ "$1" != "${SHELL_PID}" ]; then
      kill -TERM "$1" 2>/dev/null || true
      return 0
    fi
    if [ "${scenario}" = shutdown-137 ]; then
      kill -KILL "${SHELL_PID}"
    else
      : > "${EXIT_MARKER}"
      while kill -0 "${SHELL_PID}" 2>/dev/null; do command sleep 0.01; done
    fi
  }
  ;;
esac
''' + tail)
PY

for scenario in settle-134 settle-0 shutdown-134; do
  if bash "${fixture}/liveness.sh" "${scenario}" "${fixture}/${scenario}" \
    >"${fixture}/${scenario}.stdout" 2>"${fixture}/${scenario}.stderr"; then
    echo "offline harness accepted unexpected shell exit: ${scenario}" >&2
    cat "${fixture}/${scenario}.stdout" >&2
    exit 1
  fi
  grep -Eq 'shell.*exited unexpectedly' "${fixture}/${scenario}.stderr"
  echo "offline liveness rejected unexpected shell exit: ${scenario}"
done
for scenario in alive shutdown-0 shutdown-137; do
  bash "${fixture}/liveness.sh" "${scenario}" "${fixture}/${scenario}" \
    >"${fixture}/${scenario}.stdout" 2>"${fixture}/${scenario}.stderr"
  grep -Fq 'native-shell macOS offline smoke passed' "${fixture}/${scenario}.stdout"
  echo "offline liveness accepted intentional shutdown: ${scenario}"
done

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
