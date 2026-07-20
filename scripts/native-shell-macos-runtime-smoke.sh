#!/usr/bin/env bash
# Prove the installed macOS package lifecycle: mount the built DMG, install the
# app by copying it out, and drive launch/relaunch/close/uninstall/reinstall
# against an isolated SAGE_HOME. Written for bash 3.2 so it runs against the
# system bash on an Apple runner.
set -euo pipefail

if [ "$(uname -s)" != "Darwin" ]; then
  echo "native-shell installed runtime smoke requires macOS" >&2
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

# list_matches tolerates a missing directory: under `set -o pipefail` a failing
# find would otherwise abort the script before any diagnostic could be printed.
list_matches() {
  local dir=$1
  shift
  if [ ! -d "${dir}" ]; then
    return 0
  fi
  find "${dir}" "$@" 2>/dev/null || true
}

# exactly_one <label> <newline-separated matches>
exactly_one() {
  local label=$1 matches=$2 count
  count=$(printf '%s' "${matches}" | grep -c . || true)
  if [ "${count}" -ne 1 ]; then
    echo "expected exactly one ${label}, found ${count}" >&2
    return 1
  fi
  printf '%s\n' "${matches}" | head -1
}

DMG_PACKAGE=$(exactly_one 'DMG package' \
  "$(list_matches "${BUNDLE_ROOT}/dmg" -maxdepth 1 -type f -name '*.dmg')")

STAGED_APP=$(exactly_one 'staged macOS app' \
  "$(list_matches "${BUNDLE_ROOT}/macos" -maxdepth 1 -type d -name '*.app')")

# Canonicalize immediately: mktemp hands back the unresolved /var form while the
# kernel reports exec'd binaries under /private/var, and the daemon-identity
# check below compares those paths literally.
SMOKE_ROOT=$(cd "$(mktemp -d "${RUNNER_TEMP:-/tmp}/sage-native-shell-macos-runtime.XXXXXX")" && pwd -P)
SAGE_SMOKE_HOME="${SMOKE_ROOT}/home"
INSTALL_ROOT="${SMOKE_ROOT}/Applications"
MOUNT_POINT="${SMOKE_ROOT}/mnt"
DIAGNOSTICS="${SMOKE_ROOT}/diagnostics"
SHELL_LOG="${DIAGNOSTICS}/shell.log"
mkdir -p "${SAGE_SMOKE_HOME}" "${INSTALL_ROOT}" "${DIAGNOSTICS}"
printf 'native-shell-uninstall-preservation\n' > "${SAGE_SMOKE_HOME}/preserve.sentinel"

MOUNTED=0
TRACKED_PIDS=""

track_pid() {
  case " ${TRACKED_PIDS} " in
    *" $1 "*) ;;
    *) TRACKED_PIDS="${TRACKED_PIDS} $1" ;;
  esac
}

process_path() {
  ps -p "$1" -o comm= 2>/dev/null || true
}

# stop_exact_pid mirrors the Windows harness Stop-ExactTree containment rule:
# never signal a PID whose executable path is not the exact one we launched or
# derived from the control socket.
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

detach_dmg() {
  if [ "${MOUNTED}" -eq 1 ]; then
    hdiutil detach "${MOUNT_POINT}" -quiet 2>/dev/null ||
      hdiutil detach "${MOUNT_POINT}" -force -quiet 2>/dev/null || true
    MOUNTED=0
  fi
}

# discover_daemon_pid derives a serving daemon PID from the control socket alone,
# with no status validation. Cleanup needs this because an early failure (the
# daemon came up but never reported a valid status) would otherwise leak the
# process it started: the PID is only tracked once query_status returns.
discover_daemon_pid() {
  python3 - "${SAGE_SMOKE_HOME}/run/shell-control.sock" 2>/dev/null <<'PY' || true
import socket
import sys

SOL_LOCAL = 0
LOCAL_PEERPID = 2
try:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
        stream.settimeout(2)
        stream.connect(sys.argv[1])
        pid = int.from_bytes(stream.getsockopt(SOL_LOCAL, LOCAL_PEERPID, 4), sys.byteorder)
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
  stray=$(discover_daemon_pid)
  if [ -n "${stray}" ]; then
    track_pid "${stray}"
  fi
  for pid in ${TRACKED_PIDS}; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null
    fi
  done
  sleep 0.5
  for pid in ${TRACKED_PIDS}; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill -KILL "${pid}" 2>/dev/null
    fi
  done
  detach_dmg
  echo "native-shell macOS runtime smoke evidence: ${DIAGNOSTICS}"
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

# query_status validates the exact SSCP status schema and additionally derives
# the serving daemon PID from the connected socket with LOCAL_PEERPID -- the
# macOS analogue of GetNamedPipeServerProcessId. Nothing here matches on a
# process name.
query_status() {
  python3 - "${SAGE_SMOKE_HOME}/run/shell-control.sock" "${EXPECTED_UI_ORIGIN}" "${EXPECTED_VERSION}" <<'PY'
import json
import re
import socket
import struct
import sys
import time

endpoint = sys.argv[1]
expected_origin = sys.argv[2]
expected_version = sys.argv[3]
SOL_LOCAL = 0
LOCAL_PEERPID = 2
request = json.dumps(
    {"control_protocol": 1, "shell_protocol": 1, "operation": "status"},
    separators=(",", ":"),
).encode()
deadline = time.monotonic() + 60
last_error = None
expected_fields = {
    "control_protocol",
    "daemon_version",
    "api_schema",
    "min_shell_protocol",
    "max_shell_protocol",
    "instance_generation",
    "state",
    "ui_origin",
    "startup_proof",
}
canonical_generation_last = set("AEIMQUYcgkosw048")


def validate(status):
    if not isinstance(status, dict) or set(status) != expected_fields:
        raise ValueError("SSCP status schema is not exact")
    if type(status["control_protocol"]) is not int or type(status["api_schema"]) is not int:
        raise ValueError("SSCP control/API protocol has the wrong type")
    if status["control_protocol"] != 1 or status["api_schema"] != 1:
        raise ValueError("SSCP control/API protocol mismatch")
    minimum = status["min_shell_protocol"]
    maximum = status["max_shell_protocol"]
    if type(minimum) is not int or type(maximum) is not int or not (minimum <= 1 <= maximum):
        raise ValueError("SSCP shell protocol is incompatible")
    if minimum > maximum:
        raise ValueError("SSCP shell protocol range is inverted")
    version = status["daemon_version"]
    if not isinstance(version, str) or version != expected_version:
        raise ValueError(f"SSCP daemon version is unsupported: {version!r}")
    generation = status["instance_generation"]
    if not isinstance(generation, str) or not re.fullmatch(r"[A-Za-z0-9_-]{43}", generation):
        raise ValueError("SSCP generation is malformed")
    if generation[-1] not in canonical_generation_last:
        raise ValueError("SSCP generation is not canonical base64url")
    if not isinstance(status["state"], str) or status["state"] not in {"ready", "degraded"}:
        raise ValueError(f"SSCP daemon is not renderable: {status['state']!r}")
    if not isinstance(status["ui_origin"], str) or status["ui_origin"] != expected_origin:
        raise ValueError("SSCP UI origin does not match the isolated REST listener")
    proof = status["startup_proof"]
    if not isinstance(proof, str) or not re.fullmatch(r"[0-9a-f]{64}", proof):
        raise ValueError("SSCP startup proof is missing or malformed")


def read_exact(stream, size):
    chunks = []
    remaining = size
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
            raw_pid = stream.getsockopt(SOL_LOCAL, LOCAL_PEERPID, 4)
            daemon_pid = int.from_bytes(raw_pid, sys.byteorder)
            if daemon_pid <= 0:
                raise RuntimeError("SSCP peer pid is not derivable")
            stream.sendall(struct.pack(">I", len(request)) + request)
            size = struct.unpack(">I", read_exact(stream, 4))[0]
            if size > 16 * 1024:
                raise RuntimeError(f"oversized SSCP response: {size}")
            status = json.loads(read_exact(stream, size))
            validate(status)
            print(json.dumps({"status": status, "daemon_pid": daemon_pid}, sort_keys=True))
            raise SystemExit(0)
    except (FileNotFoundError, ConnectionRefusedError, TimeoutError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        last_error = error
        time.sleep(0.1)

raise SystemExit(f"timed out waiting for authenticated SSCP status: {last_error}")
PY
}

status_generation() {
  python3 -c 'import json,sys; print(json.load(sys.stdin)["status"]["instance_generation"])'
}

status_daemon_pid() {
  python3 -c 'import json,sys; print(json.load(sys.stdin)["daemon_pid"])'
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

require_relaunch_exit() {
  local pid=$1 deadline=$((SECONDS + 20)) status
  while kill -0 "${pid}" 2>/dev/null && [ "${SECONDS}" -lt "${deadline}" ]; do
    sleep 0.1
  done
  if kill -0 "${pid}" 2>/dev/null; then
    echo "second native-shell launch did not hand off to the existing instance" >&2
    return 1
  fi
  set +e
  wait "${pid}"
  status=$?
  set -e
  if [ "${status}" -ne 0 ]; then
    echo "second native-shell launch exited with status ${status}" >&2
    return 1
  fi
}

require_socket_gone() {
  local deadline=$((SECONDS + 10))
  while [ "${SECONDS}" -lt "${deadline}" ] && [ -S "${SAGE_SMOKE_HOME}/run/shell-control.sock" ]; do
    sleep 0.1
  done
  if [ -S "${SAGE_SMOKE_HOME}/run/shell-control.sock" ]; then
    echo "control socket outlived the daemon it belonged to" >&2
    return 1
  fi
}

app_executable() {
  local app=$1
  exactly_one 'installed shell executable' \
    "$(list_matches "${app}/Contents/MacOS" -maxdepth 1 -type f)"
}

install_from_dmg() {
  local mounted_app
  detach_dmg
  rm -rf "${MOUNT_POINT}"
  mkdir -p "${MOUNT_POINT}"
  hdiutil attach "${DMG_PACKAGE}" -nobrowse -readonly -noautoopen -mountpoint "${MOUNT_POINT}" >/dev/null
  MOUNTED=1
  mounted_app=$(exactly_one 'app inside the DMG' \
    "$(list_matches "${MOUNT_POINT}" -maxdepth 1 -type d -name '*.app')")
  rm -rf "${INSTALL_ROOT:?}/$(basename "${mounted_app}")"
  ditto "${mounted_app}" "${INSTALL_ROOT}/$(basename "${mounted_app}")"
  detach_dmg
  printf '%s\n' "${INSTALL_ROOT}/$(basename "${mounted_app}")"
}

{
  printf '{\n'
  printf '  "schema": "dev.sage.native-shell.macos-runtime/v1",\n'
  printf '  "os_version": "%s",\n' "$(sw_vers -productVersion)"
  printf '  "os_build": "%s",\n' "$(sw_vers -buildVersion)"
  printf '  "arch": "%s",\n' "$(uname -m)"
  printf '  "webkit_system_version": "%s",\n' \
    "$(defaults read /System/Library/Frameworks/WebKit.framework/Resources/Info.plist CFBundleVersion 2>/dev/null || echo unknown)"
  printf '  "expected_version": "%s"\n' "${EXPECTED_VERSION}"
  printf '}\n'
} > "${DIAGNOSTICS}/preflight.json"

INSTALLED_APP=$(install_from_dmg)
INSTALLED_EXECUTABLE=$(app_executable "${INSTALLED_APP}")
INSTALLED_DAEMON="${INSTALLED_APP}/Contents/Resources/binaries/sage-gui"
test -x "${INSTALLED_DAEMON}"

# --- installed DMG copy: launch, relaunch handoff, close, daemon survival ---
launch_shell "${INSTALLED_EXECUTABLE}"
INSTALLED_SHELL_PID=${LAUNCHED_PID}
FIRST_STATUS=$(query_status)
printf '%s\n' "${FIRST_STATUS}" > "${DIAGNOSTICS}/first-install.json"
FIRST_GENERATION=$(printf '%s\n' "${FIRST_STATUS}" | status_generation)
FIRST_DAEMON_PID=$(printf '%s\n' "${FIRST_STATUS}" | status_daemon_pid)
track_pid "${FIRST_DAEMON_PID}"
kill -0 "${INSTALLED_SHELL_PID}"

# The socket-derived daemon must be the exact binary bundled in the installed app.
FIRST_DAEMON_PATH=$(process_path "${FIRST_DAEMON_PID}")
if [ "${FIRST_DAEMON_PATH}" != "${INSTALLED_DAEMON}" ]; then
  echo "installed shell served SSCP from an unexpected daemon: ${FIRST_DAEMON_PATH}" >&2
  exit 1
fi

test -d "${SAGE_SMOKE_HOME}/run"
test ! -L "${SAGE_SMOKE_HOME}/run"
test "$(stat -f '%A' "${SAGE_SMOKE_HOME}/run")" = 700
test -S "${SAGE_SMOKE_HOME}/run/shell-control.sock"
test ! -L "${SAGE_SMOKE_HOME}/run/shell-control.sock"
test "$(stat -f '%A' "${SAGE_SMOKE_HOME}/run/shell-control.sock")" = 600

launch_shell "${INSTALLED_EXECUTABLE}"
require_relaunch_exit "${LAUNCHED_PID}"
SECOND_GENERATION=$(query_status | status_generation)
test "${SECOND_GENERATION}" = "${FIRST_GENERATION}"
kill -0 "${INSTALLED_SHELL_PID}"

stop_exact_pid "${INSTALLED_SHELL_PID}" "${INSTALLED_EXECUTABLE}"
wait "${INSTALLED_SHELL_PID}" 2>/dev/null || true
test "$(query_status | status_generation)" = "${FIRST_GENERATION}"

# --- uninstall: app removed, ~/.sage preserved, daemon untouched ---
rm -rf "${INSTALLED_APP}"
test ! -e "${INSTALLED_EXECUTABLE}"
test ! -e "${INSTALLED_APP}"
grep -Fxq 'native-shell-uninstall-preservation' "${SAGE_SMOKE_HOME}/preserve.sentinel"
test "$(query_status | status_generation)" = "${FIRST_GENERATION}"

stop_exact_pid "${FIRST_DAEMON_PID}" ""
require_socket_gone

# --- staged .app bundle: a distinct generation proves a genuinely new daemon ---
STAGED_EXECUTABLE=$(app_executable "${STAGED_APP}")
launch_shell "${STAGED_EXECUTABLE}"
STAGED_SHELL_PID=${LAUNCHED_PID}
STAGED_STATUS=$(query_status)
STAGED_GENERATION=$(printf '%s\n' "${STAGED_STATUS}" | status_generation)
STAGED_DAEMON_PID=$(printf '%s\n' "${STAGED_STATUS}" | status_daemon_pid)
track_pid "${STAGED_DAEMON_PID}"
kill -0 "${STAGED_SHELL_PID}"
test "${STAGED_GENERATION}" != "${FIRST_GENERATION}"
test "${STAGED_DAEMON_PID}" != "${FIRST_DAEMON_PID}"

launch_shell "${STAGED_EXECUTABLE}"
require_relaunch_exit "${LAUNCHED_PID}"
test "$(query_status | status_generation)" = "${STAGED_GENERATION}"
kill -0 "${STAGED_SHELL_PID}"

stop_exact_pid "${STAGED_SHELL_PID}" "${STAGED_EXECUTABLE}"
wait "${STAGED_SHELL_PID}" 2>/dev/null || true
test "$(query_status | status_generation)" = "${STAGED_GENERATION}"
stop_exact_pid "${STAGED_DAEMON_PID}" ""
require_socket_gone

# --- reinstall over preserved state ---
REINSTALLED_APP=$(install_from_dmg)
REINSTALLED_EXECUTABLE=$(app_executable "${REINSTALLED_APP}")
test -x "${REINSTALLED_EXECUTABLE}"
grep -Fxq 'native-shell-uninstall-preservation' "${SAGE_SMOKE_HOME}/preserve.sentinel"

launch_shell "${REINSTALLED_EXECUTABLE}"
REINSTALLED_SHELL_PID=${LAUNCHED_PID}
REINSTALLED_STATUS=$(query_status)
printf '%s\n' "${REINSTALLED_STATUS}" > "${DIAGNOSTICS}/reinstall.json"
REINSTALLED_GENERATION=$(printf '%s\n' "${REINSTALLED_STATUS}" | status_generation)
REINSTALLED_DAEMON_PID=$(printf '%s\n' "${REINSTALLED_STATUS}" | status_daemon_pid)
track_pid "${REINSTALLED_DAEMON_PID}"
kill -0 "${REINSTALLED_SHELL_PID}"
test "${REINSTALLED_GENERATION}" != "${FIRST_GENERATION}"
test "${REINSTALLED_GENERATION}" != "${STAGED_GENERATION}"

stop_exact_pid "${REINSTALLED_SHELL_PID}" "${REINSTALLED_EXECUTABLE}"
wait "${REINSTALLED_SHELL_PID}" 2>/dev/null || true
test "$(query_status | status_generation)" = "${REINSTALLED_GENERATION}"
stop_exact_pid "${REINSTALLED_DAEMON_PID}" ""
require_socket_gone

rm -rf "${REINSTALLED_APP}"
test ! -e "${REINSTALLED_EXECUTABLE}"
grep -Fxq 'native-shell-uninstall-preservation' "${SAGE_SMOKE_HOME}/preserve.sentinel"

if [ -f "${SHELL_LOG}" ]; then
  tail -n 400 "${SHELL_LOG}" > "${DIAGNOSTICS}/shell-tail.log"
fi

echo "native-shell macOS installed runtime smoke passed dmg=${FIRST_GENERATION} app=${STAGED_GENERATION} reinstall=${REINSTALLED_GENERATION}"
