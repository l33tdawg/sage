#!/usr/bin/env bash
set -euo pipefail

if [ "$(uname -s)" != "Linux" ]; then
  echo "native-shell installed runtime smoke requires Linux" >&2
  exit 1
fi
if [ "$#" -ne 1 ]; then
  echo "usage: $0 <tauri-bundle-root>" >&2
  exit 1
fi

BUNDLE_ROOT=$1
if [ ! -d "${BUNDLE_ROOT}" ]; then
  echo "native-shell bundle root does not exist: ${BUNDLE_ROOT}" >&2
  exit 1
fi

exactly_one() {
  local label=$1
  shift
  local -a matches=("$@")
  if [ "${#matches[@]}" -ne 1 ]; then
    echo "expected exactly one ${label}, found ${#matches[@]}" >&2
    return 1
  fi
  printf '%s\n' "${matches[0]}"
}

mapfile -d '' -t deb_matches < <(find "${BUNDLE_ROOT}" -type f -name '*.deb' -print0)
mapfile -d '' -t appimage_matches < <(find "${BUNDLE_ROOT}" -type f -name '*.AppImage' -print0)
DEB_PACKAGE=$(exactly_one 'Debian package' "${deb_matches[@]}")
APPIMAGE=$(exactly_one 'AppImage package' "${appimage_matches[@]}")
chmod +x "${APPIMAGE}"

SMOKE_ROOT=$(mktemp -d "${RUNNER_TEMP:-/tmp}/sage-native-shell-runtime.XXXXXX")
SAGE_SMOKE_HOME="${SMOKE_ROOT}/home"
SHELL_LOG="${SMOKE_ROOT}/shell.log"
mkdir -p "${SAGE_SMOKE_HOME}"
printf 'native-shell-uninstall-preservation\n' > "${SAGE_SMOKE_HOME}/preserve.sentinel"
PACKAGE_NAME=$(dpkg-deb -f "${DEB_PACKAGE}" Package)
if dpkg-query -W -f='${db:Status-Abbrev}' "${PACKAGE_NAME}" 2>/dev/null | grep -q '^ii'; then
  echo "refusing to replace an already-installed package: ${PACKAGE_NAME}" >&2
  exit 1
fi

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

matching_smoke_pids() {
  local environ pid
  for environ in /proc/[0-9]*/environ; do
    [ -r "${environ}" ] || continue
    if tr '\0' '\n' < "${environ}" 2>/dev/null |
      grep -Fxq "SAGE_HOME=${SAGE_SMOKE_HOME}"; then
      pid=${environ#/proc/}
      printf '%s\n' "${pid%%/*}"
    fi
  done
}

stop_smoke_processes() {
  local pid deadline
  while IFS= read -r pid; do
    [ -n "${pid}" ] && kill -TERM "${pid}" 2>/dev/null || true
  done < <(matching_smoke_pids)
  deadline=$((SECONDS + 5))
  while [ "${SECONDS}" -lt "${deadline}" ] && [ -n "$(matching_smoke_pids)" ]; do
    sleep 0.1
  done
  while IFS= read -r pid; do
    [ -n "${pid}" ] && kill -KILL "${pid}" 2>/dev/null || true
  done < <(matching_smoke_pids)
}

cleanup() {
  local original_status=$1 removal_status=0
  trap - EXIT
  set +e
  stop_smoke_processes
  if dpkg-query -W "${PACKAGE_NAME}" >/dev/null 2>&1; then
    sudo dpkg --remove "${PACKAGE_NAME}" >/dev/null 2>&1
    removal_status=$?
  fi
  echo "native-shell runtime smoke evidence: ${SMOKE_ROOT}"
  if [ "${original_status}" -eq 0 ] && [ "${removal_status}" -ne 0 ]; then
    echo "failed to remove ${PACKAGE_NAME} during native-shell runtime cleanup" >&2
    original_status=${removal_status}
  fi
  exit "${original_status}"
}
trap 'cleanup $?' EXIT
trap 'exit 130' INT TERM

query_status() {
  python3 - "${SAGE_SMOKE_HOME}/run/shell-control.sock" "${EXPECTED_UI_ORIGIN}" <<'PY'
import json
import re
import socket
import struct
import sys
import time

endpoint = sys.argv[1]
expected_origin = sys.argv[2]
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
    if not isinstance(version, str) or not re.fullmatch(
        r"v?11\.(?:10|11)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?",
        version,
    ):
        raise ValueError("SSCP daemon version is unsupported")
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
            stream.sendall(struct.pack(">I", len(request)) + request)
            size = struct.unpack(">I", read_exact(stream, 4))[0]
            if size > 16 * 1024:
                raise RuntimeError(f"oversized SSCP response: {size}")
            status = json.loads(read_exact(stream, size))
            validate(status)
            print(json.dumps(status, sort_keys=True))
            raise SystemExit(0)
    except (FileNotFoundError, ConnectionRefusedError, TimeoutError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        last_error = error
        time.sleep(0.1)

raise SystemExit(f"timed out waiting for authenticated SSCP status: {last_error}")
PY
}

status_generation() {
  python3 -c '
import json, re, sys
status = json.load(sys.stdin)
generation = status["instance_generation"]
print(generation)
'
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
}

require_relaunch_exit() {
  local pid=$1 deadline=$((SECONDS + 10)) status
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

sudo dpkg -i "${DEB_PACKAGE}"
mapfile -t installed_executables < <(
  dpkg -L "${PACKAGE_NAME}" |
    while IFS= read -r candidate; do
      if [[ "${candidate}" == /usr/bin/* ]] && [ -f "${candidate}" ] && [ -x "${candidate}" ]; then
        printf '%s\n' "${candidate}"
      fi
    done
)
INSTALLED_EXECUTABLE=$(exactly_one 'installed native-shell executable' "${installed_executables[@]}")

launch_shell "${INSTALLED_EXECUTABLE}"
INSTALLED_SHELL_PID=${LAUNCHED_PID}
FIRST_STATUS=$(query_status)
FIRST_GENERATION=$(printf '%s\n' "${FIRST_STATUS}" | status_generation)
kill -0 "${INSTALLED_SHELL_PID}"
test -d "${SAGE_SMOKE_HOME}/run"
test ! -L "${SAGE_SMOKE_HOME}/run"
test "$(stat -c '%a' "${SAGE_SMOKE_HOME}/run")" = 700
test -S "${SAGE_SMOKE_HOME}/run/shell-control.sock"
test ! -L "${SAGE_SMOKE_HOME}/run/shell-control.sock"
test "$(stat -c '%a' "${SAGE_SMOKE_HOME}/run/shell-control.sock")" = 600

launch_shell "${INSTALLED_EXECUTABLE}"
require_relaunch_exit "${LAUNCHED_PID}"
SECOND_GENERATION=$(query_status | status_generation)
test "${SECOND_GENERATION}" = "${FIRST_GENERATION}"
kill -0 "${INSTALLED_SHELL_PID}"

kill -TERM "${INSTALLED_SHELL_PID}"
wait "${INSTALLED_SHELL_PID}" 2>/dev/null || true
test "$(query_status | status_generation)" = "${FIRST_GENERATION}"

sudo dpkg --remove "${PACKAGE_NAME}"
test ! -e "${INSTALLED_EXECUTABLE}"
if dpkg-query -W -f='${db:Status-Abbrev}' "${PACKAGE_NAME}" 2>/dev/null | grep -q '^ii'; then
  echo "Debian package remained installed after removal: ${PACKAGE_NAME}" >&2
  exit 1
fi
grep -Fx 'native-shell-uninstall-preservation' "${SAGE_SMOKE_HOME}/preserve.sentinel"
test "$(query_status | status_generation)" = "${FIRST_GENERATION}"

stop_smoke_processes
test ! -S "${SAGE_SMOKE_HOME}/run/shell-control.sock"

launch_shell env APPIMAGE_EXTRACT_AND_RUN=1 "${APPIMAGE}"
APPIMAGE_SHELL_PID=${LAUNCHED_PID}
APPIMAGE_GENERATION=$(query_status | status_generation)
kill -0 "${APPIMAGE_SHELL_PID}"
test "${APPIMAGE_GENERATION}" != "${FIRST_GENERATION}"

launch_shell env APPIMAGE_EXTRACT_AND_RUN=1 "${APPIMAGE}"
require_relaunch_exit "${LAUNCHED_PID}"
test "$(query_status | status_generation)" = "${APPIMAGE_GENERATION}"
kill -0 "${APPIMAGE_SHELL_PID}"

kill -TERM "${APPIMAGE_SHELL_PID}"
wait "${APPIMAGE_SHELL_PID}" 2>/dev/null || true
test "$(query_status | status_generation)" = "${APPIMAGE_GENERATION}"
stop_smoke_processes
test ! -S "${SAGE_SMOKE_HOME}/run/shell-control.sock"

sudo dpkg -i "${DEB_PACKAGE}"
test -x "${INSTALLED_EXECUTABLE}"
grep -Fx 'native-shell-uninstall-preservation' "${SAGE_SMOKE_HOME}/preserve.sentinel"
launch_shell "${INSTALLED_EXECUTABLE}"
REINSTALLED_SHELL_PID=${LAUNCHED_PID}
REINSTALLED_GENERATION=$(query_status | status_generation)
kill -0 "${REINSTALLED_SHELL_PID}"
test "${REINSTALLED_GENERATION}" != "${FIRST_GENERATION}"
test "${REINSTALLED_GENERATION}" != "${APPIMAGE_GENERATION}"

kill -TERM "${REINSTALLED_SHELL_PID}"
wait "${REINSTALLED_SHELL_PID}" 2>/dev/null || true
test "$(query_status | status_generation)" = "${REINSTALLED_GENERATION}"
stop_smoke_processes
test ! -S "${SAGE_SMOKE_HOME}/run/shell-control.sock"
sudo dpkg --remove "${PACKAGE_NAME}"
test ! -e "${INSTALLED_EXECUTABLE}"
grep -Fx 'native-shell-uninstall-preservation' "${SAGE_SMOKE_HOME}/preserve.sentinel"

echo "native-shell Linux installed runtime smoke passed deb=${FIRST_GENERATION} appimage=${APPIMAGE_GENERATION} reinstall=${REINSTALLED_GENERATION}"
