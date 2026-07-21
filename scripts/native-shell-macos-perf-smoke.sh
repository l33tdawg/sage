#!/usr/bin/env bash
# Measure the v11.11 performance rows that are observable without shell-side
# instrumentation: incremental shell RSS (blocking) and settled idle CPU
# (recorded only).
#
# RSS blocks because it is the premise of the framework decision, not because it
# is convenient: desktop-shell-decision.md rejected Electron at 358,720 KiB
# against this exact 200 MiB ceiling and selected Tauri at 142,544 KiB. The
# other seven budgets need paint/interactive/frame signals the shell does not
# emit, and become blocking in v11.14. See docs/native-shell-quality-gates.md.
#
# Written for bash 3.2 so it runs against the system bash on an Apple runner.
set -euo pipefail

if [ "$(uname -s)" != "Darwin" ]; then
  echo "native-shell performance smoke requires macOS" >&2
  exit 1
fi
if [ "$#" -ne 2 ]; then
  echo "usage: $0 <tauri-bundle-root> <expected-version>" >&2
  exit 1
fi

BUNDLE_ROOT=$1
EXPECTED_VERSION=$2
if [ ! -d "${BUNDLE_ROOT}" ]; then
  echo "native-shell bundle root does not exist: ${BUNDLE_ROOT}" >&2
  exit 1
fi
BUNDLE_ROOT=$(cd "${BUNDLE_ROOT}" && pwd -P)

# 200 MiB, the ceiling that vetoed Electron in the framework decision.
RSS_CEILING_KIB=204800

list_matches() {
  local dir=$1
  shift
  if [ ! -d "${dir}" ]; then
    return 0
  fi
  find "${dir}" "$@" 2>/dev/null || true
}

exactly_one() {
  local label=$1 matches=$2 count
  count=$(printf '%s' "${matches}" | grep -c . || true)
  if [ "${count}" -ne 1 ]; then
    echo "expected exactly one ${label}, found ${count}" >&2
    return 1
  fi
  printf '%s\n' "${matches}" | head -1
}

STAGED_APP=$(exactly_one 'staged macOS app' \
  "$(list_matches "${BUNDLE_ROOT}/macos" -maxdepth 1 -type d -name '*.app')")
SHELL_EXECUTABLE=$(exactly_one 'shell executable' \
  "$(list_matches "${STAGED_APP}/Contents/MacOS" -maxdepth 1 -type f)")
BUNDLED_DAEMON="${STAGED_APP}/Contents/Resources/binaries/sage-gui"
test -x "${BUNDLED_DAEMON}"

SMOKE_ROOT=$(cd "$(mktemp -d "${RUNNER_TEMP:-/tmp}/sage-native-shell-macos-perf.XXXXXX")" && pwd -P)
SAGE_SMOKE_HOME="${SMOKE_ROOT}/home"
DIAGNOSTICS="${SMOKE_ROOT}/diagnostics"
SHELL_LOG="${DIAGNOSTICS}/shell.log"
SAMPLES="${DIAGNOSTICS}/process-samples.txt"
mkdir -p "${SAGE_SMOKE_HOME}" "${DIAGNOSTICS}"
: > "${SAMPLES}"

TRACKED_PIDS=""
SAMPLER_PID=""

track_pid() {
  case " ${TRACKED_PIDS} " in
    *" $1 "*) ;;
    *) TRACKED_PIDS="${TRACKED_PIDS} $1" ;;
  esac
}

process_path() {
  ps -p "$1" -o comm= 2>/dev/null || true
}

stop_exact_pid() {
  local pid=$1 expected=$2 actual deadline
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 0
  fi
  actual=$(process_path "${pid}")
  if [ -n "${expected}" ] && [ -n "${actual}" ] && [ "${actual}" != "${expected}" ]; then
    echo "refusing to signal pid ${pid}: expected ${expected}, found ${actual}" >&2
    return 1
  fi
  kill -TERM "${pid}" 2>/dev/null || true
  deadline=$((SECONDS + 10))
  while [ "${SECONDS}" -lt "${deadline}" ] && kill -0 "${pid}" 2>/dev/null; do
    sleep 0.1
  done
  if kill -0 "${pid}" 2>/dev/null; then
    kill -KILL "${pid}" 2>/dev/null || true
  fi
}

discover_daemon_pid() {
  python3 - "${SAGE_SMOKE_HOME}/run/shell-control.sock" 2>/dev/null <<'PY' || true
import socket
import sys

try:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
        stream.settimeout(2)
        stream.connect(sys.argv[1])
        pid = int.from_bytes(stream.getsockopt(0, 2, 4), sys.byteorder)
        if pid > 0:
            print(pid)
except OSError:
    pass
PY
}

cleanup() {
  local original_status=$1 pid stray
  trap - EXIT
  set +e
  if [ -n "${SAMPLER_PID}" ] && kill -0 "${SAMPLER_PID}" 2>/dev/null; then
    kill -TERM "${SAMPLER_PID}" 2>/dev/null
  fi
  stray=$(discover_daemon_pid)
  if [ -n "${stray}" ]; then
    track_pid "${stray}"
  fi
  for pid in ${TRACKED_PIDS}; do
    kill -0 "${pid}" 2>/dev/null && kill -TERM "${pid}" 2>/dev/null
  done
  sleep 0.5
  for pid in ${TRACKED_PIDS}; do
    kill -0 "${pid}" 2>/dev/null && kill -KILL "${pid}" 2>/dev/null
  done
  echo "native-shell macOS performance evidence: ${DIAGNOSTICS}"
  exit "${original_status}"
}
trap 'cleanup $?' EXIT
trap 'exit 130' INT TERM

read -r REST_PORT RPC_PORT P2P_PORT < <(python3 - <<'PY'
import socket

sockets = []
try:
    for _ in range(3):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        sockets.append(listener)
    print(*(listener.getsockname()[1] for listener in sockets))
finally:
    for listener in sockets:
        listener.close()
PY
)
EXPECTED_UI_ORIGIN="http://127.0.0.1:${REST_PORT}"

query_status() {
  python3 - "${SAGE_SMOKE_HOME}/run/shell-control.sock" "${EXPECTED_UI_ORIGIN}" "${EXPECTED_VERSION}" <<'PY'
import json
import re
import socket
import struct
import sys
import time

endpoint, expected_origin, expected_version = sys.argv[1], sys.argv[2], sys.argv[3]
request = json.dumps(
    {"control_protocol": 1, "shell_protocol": 1, "operation": "status"},
    separators=(",", ":"),
).encode()
deadline = time.monotonic() + 90
last_error = None
expected_fields = {
    "control_protocol", "daemon_version", "api_schema", "min_shell_protocol",
    "max_shell_protocol", "instance_generation", "state", "ui_origin", "startup_proof",
}


def validate(status):
    if not isinstance(status, dict) or set(status) != expected_fields:
        raise ValueError("SSCP status schema is not exact")
    if status["daemon_version"] != expected_version:
        raise ValueError(f"SSCP daemon version is unsupported: {status['daemon_version']!r}")
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}", status["instance_generation"]):
        raise ValueError("SSCP generation is malformed")
    if status["state"] not in {"ready", "degraded"}:
        raise ValueError(f"SSCP daemon is not renderable: {status['state']!r}")
    if status["ui_origin"] != expected_origin:
        raise ValueError("SSCP UI origin does not match the isolated REST listener")


def read_exact(stream, size):
    chunks, remaining = [], size
    while remaining:
        chunk = stream.recv(remaining)
        if not chunk:
            raise RuntimeError("SSCP connection closed before the frame completed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


while time.monotonic() < deadline:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
            stream.settimeout(1)
            stream.connect(endpoint)
            daemon_pid = int.from_bytes(stream.getsockopt(0, 2, 4), sys.byteorder)
            stream.sendall(struct.pack(">I", len(request)) + request)
            size = struct.unpack(">I", read_exact(stream, 4))[0]
            status = json.loads(read_exact(stream, size))
            validate(status)
            print(json.dumps({"status": status, "daemon_pid": daemon_pid}, sort_keys=True))
            raise SystemExit(0)
    except (FileNotFoundError, ConnectionRefusedError, TimeoutError, OSError,
            RuntimeError, ValueError, json.JSONDecodeError) as error:
        last_error = error
        time.sleep(0.1)

raise SystemExit(f"timed out waiting for a renderable SSCP status: {last_error}")
PY
}

# Sample RSS and CPU for every process running the exact shell executable, and
# separately for the exact bundled daemon so the daemon can be excluded from the
# shell budget while still being recorded for context. Exact absolute executable
# path only; never a process name.
sample_processes() {
  while :; do
    python3 - "${SHELL_EXECUTABLE}" "${BUNDLED_DAEMON}" >> "${SAMPLES}" 2>/dev/null <<'PY' || true
import subprocess
import sys
import time

shell_path, daemon_path = sys.argv[1], sys.argv[2]
try:
    listing = subprocess.run(["ps", "-Ao", "pid=,ppid=,rss=,%cpu=,comm="],
                             capture_output=True, text=True, timeout=5).stdout
except Exception:
    raise SystemExit(0)
stamp = time.monotonic()
for line in listing.splitlines():
    parts = line.strip().split(None, 4)
    if len(parts) < 5:
        continue
    pid, ppid, rss, cpu, path = parts
    if path == shell_path:
        role = "shell"
    elif path == daemon_path:
        role = "daemon"
    else:
        continue
    print(f"{stamp:.3f}\t{role}\t{pid}\t{ppid}\t{rss}\t{cpu}")
PY
    sleep 0.25
  done
}

launch_shell() {
  env \
    SAGE_HOME="${SAGE_SMOKE_HOME}" \
    SAGE_NO_BROWSER=1 \
    REST_ADDR="127.0.0.1:${REST_PORT}" \
    SAGE_CMT_RPC_ADDR="tcp://127.0.0.1:${RPC_PORT}" \
    SAGE_CMT_P2P_ADDR="tcp://127.0.0.1:${P2P_PORT}" \
    "$@" >> "${SHELL_LOG}" 2>&1 &
  LAUNCHED_PID=$!
  track_pid "${LAUNCHED_PID}"
}

launch_shell "${SHELL_EXECUTABLE}"
SHELL_PID=${LAUNCHED_PID}

STATUS=$(query_status)
printf '%s\n' "${STATUS}" > "${DIAGNOSTICS}/ready-status.json"
DAEMON_PID=$(printf '%s\n' "${STATUS}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["daemon_pid"])')
track_pid "${DAEMON_PID}"
kill -0 "${SHELL_PID}"

# Only sample once the app is renderable: the budget is a settled-state budget,
# not a startup transient.
sample_processes &
SAMPLER_PID=$!
sleep 12
kill -TERM "${SAMPLER_PID}" 2>/dev/null || true
wait "${SAMPLER_PID}" 2>/dev/null || true
SAMPLER_PID=""

python3 scripts/native-shell-perf-evaluate.py \
  "${SAMPLES}" "${RSS_CEILING_KIB}" "${DIAGNOSTICS}/performance.json" \
  "$(sw_vers -productVersion)" "$(sw_vers -buildVersion)" "$(uname -m)" "${EXPECTED_VERSION}"

stop_exact_pid "${SHELL_PID}" "${SHELL_EXECUTABLE}"
wait "${SHELL_PID}" 2>/dev/null || true
stop_exact_pid "${DAEMON_PID}" ""

echo "native-shell macOS performance smoke passed"
