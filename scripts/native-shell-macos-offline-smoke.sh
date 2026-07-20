#!/usr/bin/env bash
# Prove the native shell boots and reaches a renderable state with no external
# network, and makes zero external requests while doing it.
#
# Enforcement note: macOS has no unprivileged per-process network namespace, so
# this harness does not kernel-block egress. Instead it (a) points every proxy
# variable at a dead loopback port so any proxy-respecting client fails fast,
# and (b) continuously samples the shell's and daemon's own internet sockets
# from launch through settle, failing on any non-loopback endpoint. A one-shot
# end-state check would miss a transient boot-time request; sampling does not.
# Written for bash 3.2 so it runs against the system bash on an Apple runner.
set -euo pipefail

if [ "$(uname -s)" != "Darwin" ]; then
  echo "native-shell offline smoke requires macOS" >&2
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

# Canonicalize: mktemp returns /var/... while the kernel reports exec'd binaries
# under /private/var, and socket ownership is matched on exact executable path.
SMOKE_ROOT=$(cd "$(mktemp -d "${RUNNER_TEMP:-/tmp}/sage-native-shell-macos-offline.XXXXXX")" && pwd -P)
SAGE_SMOKE_HOME="${SMOKE_ROOT}/home"
DIAGNOSTICS="${SMOKE_ROOT}/diagnostics"
SHELL_LOG="${DIAGNOSTICS}/shell.log"
SAMPLES="${DIAGNOSTICS}/socket-samples.txt"
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
  echo "native-shell macOS offline smoke evidence: ${DIAGNOSTICS}"
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
    if status["control_protocol"] != 1 or status["api_schema"] != 1:
        raise ValueError("SSCP control/API protocol mismatch")
    if status["daemon_version"] != expected_version:
        raise ValueError(f"SSCP daemon version is unsupported: {status['daemon_version']!r}")
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}", status["instance_generation"]):
        raise ValueError("SSCP generation is malformed")
    if status["state"] not in {"ready", "degraded"}:
        raise ValueError(f"SSCP daemon is not renderable offline: {status['state']!r}")
    if status["ui_origin"] != expected_origin:
        raise ValueError("SSCP UI origin does not match the isolated REST listener")
    if not re.fullmatch(r"[0-9a-f]{64}", status["startup_proof"]):
        raise ValueError("SSCP startup proof is missing or malformed")


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
            if size > 16 * 1024:
                raise RuntimeError(f"oversized SSCP response: {size}")
            status = json.loads(read_exact(stream, size))
            validate(status)
            print(json.dumps({"status": status, "daemon_pid": daemon_pid}, sort_keys=True))
            raise SystemExit(0)
    except (FileNotFoundError, ConnectionRefusedError, TimeoutError, OSError,
            RuntimeError, ValueError, json.JSONDecodeError) as error:
        last_error = error
        time.sleep(0.1)

raise SystemExit(f"timed out waiting for an offline-renderable SSCP status: {last_error}")
PY
}

# Sample the internet sockets of every process running the exact staged shell
# executable or the exact bundled daemon. Matching is on absolute executable
# path, never on a process name.
sample_sockets() {
  while :; do
    python3 - "${SHELL_EXECUTABLE}" "${BUNDLED_DAEMON}" >> "${SAMPLES}" 2>/dev/null <<'PY' || true
import subprocess
import sys

wanted = {sys.argv[1], sys.argv[2]}
try:
    listing = subprocess.run(["ps", "-Ao", "pid=,comm="], capture_output=True, text=True, timeout=5).stdout
except Exception:
    raise SystemExit(0)
pids = []
for line in listing.splitlines():
    line = line.strip()
    if not line:
        continue
    pid, _, path = line.partition(" ")
    if path.strip() in wanted and pid.isdigit():
        pids.append(pid)
if not pids:
    raise SystemExit(0)
try:
    out = subprocess.run(["lsof", "-nP", "-a", "-i", "-p", ",".join(pids)],
                         capture_output=True, text=True, timeout=10).stdout
except Exception:
    raise SystemExit(0)
for line in out.splitlines()[1:]:
    print(line)
PY
    sleep 0.2
  done
}

launch_shell() {
  env \
    SAGE_HOME="${SAGE_SMOKE_HOME}" \
    SAGE_NO_BROWSER=1 \
    REST_ADDR="127.0.0.1:${REST_PORT}" \
    SAGE_CMT_RPC_ADDR="tcp://127.0.0.1:${RPC_PORT}" \
    SAGE_CMT_P2P_ADDR="tcp://127.0.0.1:${P2P_PORT}" \
    HTTP_PROXY="http://127.0.0.1:1" HTTPS_PROXY="http://127.0.0.1:1" \
    http_proxy="http://127.0.0.1:1" https_proxy="http://127.0.0.1:1" \
    ALL_PROXY="socks5://127.0.0.1:1" NO_PROXY="" no_proxy="" \
    "$@" >> "${SHELL_LOG}" 2>&1 &
  LAUNCHED_PID=$!
  track_pid "${LAUNCHED_PID}"
}

{
  printf '{\n'
  printf '  "schema": "dev.sage.native-shell.macos-offline/v1",\n'
  printf '  "os_version": "%s",\n' "$(sw_vers -productVersion)"
  printf '  "os_build": "%s",\n' "$(sw_vers -buildVersion)"
  printf '  "arch": "%s",\n' "$(uname -m)"
  printf '  "enforcement": "proxy-sinkhole + continuous non-loopback socket sampling (no kernel egress block on macOS)",\n'
  printf '  "expected_version": "%s"\n' "${EXPECTED_VERSION}"
  printf '}\n'
} > "${DIAGNOSTICS}/preflight.json"

sample_sockets &
SAMPLER_PID=$!

launch_shell "${SHELL_EXECUTABLE}"
SHELL_PID=${LAUNCHED_PID}

STATUS=$(query_status)
printf '%s\n' "${STATUS}" > "${DIAGNOSTICS}/offline-status.json"
DAEMON_PID=$(printf '%s\n' "${STATUS}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["daemon_pid"])')
track_pid "${DAEMON_PID}"
kill -0 "${SHELL_PID}"

# Settle: keep sampling after ready so a delayed phone-home is still caught.
sleep 5

kill -TERM "${SAMPLER_PID}" 2>/dev/null || true
wait "${SAMPLER_PID}" 2>/dev/null || true
SAMPLER_PID=""

python3 - "${SAMPLES}" <<'PY'
import re
import sys

loopback = re.compile(r"^(?:127\.\d+\.\d+\.\d+|\[?::1\]?|localhost)$")
offenders = []
seen = 0
with open(sys.argv[1]) as handle:
    for line in handle:
        line = line.rstrip("\n")
        if not line.strip():
            continue
        # lsof NAME column is the last field, e.g. 127.0.0.1:5000->127.0.0.1:6000 (ESTABLISHED)
        fields = line.split()
        name = None
        for field in fields:
            if ":" in field and ("->" in field or field.count(":") >= 1):
                name = field
        if not name:
            continue
        seen += 1
        for endpoint in name.split("->"):
            endpoint = endpoint.strip()
            if not endpoint or endpoint == "*:*":
                continue
            host = endpoint.rsplit(":", 1)[0].strip("[]")
            if host in ("*", ""):
                # A wildcard bind is an exposure, not an outbound request, but it
                # is still not loopback-confined; report it.
                offenders.append(line)
                break
            if not loopback.match(host):
                offenders.append(line)
                break

if offenders:
    print(f"native-shell made or exposed {len(offenders)} non-loopback socket(s) while offline:", file=sys.stderr)
    for entry in sorted(set(offenders))[:20]:
        print(f"  {entry}", file=sys.stderr)
    raise SystemExit(1)
print(f"offline socket sampling clean: {seen} socket observations, all loopback")
PY

stop_exact_pid "${SHELL_PID}" "${SHELL_EXECUTABLE}"
wait "${SHELL_PID}" 2>/dev/null || true
stop_exact_pid "${DAEMON_PID}" ""

echo "native-shell macOS offline smoke passed (no external sockets, daemon reached a renderable state)"
