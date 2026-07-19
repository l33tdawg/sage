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
PACKAGE_INSTALLED=0

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

cleanup() {
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
  if [ "${PACKAGE_INSTALLED}" -eq 1 ] && dpkg-query -W "${PACKAGE_NAME}" >/dev/null 2>&1; then
    sudo dpkg --remove "${PACKAGE_NAME}" >/dev/null 2>&1 || true
  fi
  echo "native-shell runtime smoke evidence: ${SMOKE_ROOT}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

query_status() {
  python3 - "${SAGE_SMOKE_HOME}/run/shell-control.sock" <<'PY'
import json
import socket
import struct
import sys
import time

endpoint = sys.argv[1]
request = json.dumps(
    {"control_protocol": 1, "shell_protocol": 1, "operation": "status"},
    separators=(",", ":"),
).encode()
deadline = time.monotonic() + 20
last_error = None

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
            print(json.dumps(status, sort_keys=True))
            raise SystemExit(0)
    except (FileNotFoundError, ConnectionRefusedError, TimeoutError, OSError, RuntimeError, json.JSONDecodeError) as error:
        last_error = error
        time.sleep(0.1)

raise SystemExit(f"timed out waiting for authenticated SSCP status: {last_error}")
PY
}

status_generation() {
  python3 -c '
import json, re, sys
status = json.load(sys.stdin)
assert status["control_protocol"] == 1
assert status["api_schema"] == 1
assert status["state"] in {"starting", "locked", "ready", "degraded", "draining", "failed"}
generation = status["instance_generation"]
assert re.fullmatch(r"[A-Za-z0-9_-]{43}", generation)
print(generation)
'
}

launch_shell() {
  env SAGE_HOME="${SAGE_SMOKE_HOME}" SAGE_NO_BROWSER=1 "$@" >> "${SHELL_LOG}" 2>&1 &
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
PACKAGE_INSTALLED=1
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

kill -TERM "${INSTALLED_SHELL_PID}"
wait "${INSTALLED_SHELL_PID}" 2>/dev/null || true
test "$(query_status | status_generation)" = "${FIRST_GENERATION}"

sudo dpkg --remove "${PACKAGE_NAME}"
PACKAGE_INSTALLED=0
grep -Fx 'native-shell-uninstall-preservation' "${SAGE_SMOKE_HOME}/preserve.sentinel"
test "$(query_status | status_generation)" = "${FIRST_GENERATION}"

launch_shell env APPIMAGE_EXTRACT_AND_RUN=1 "${APPIMAGE}"
APPIMAGE_SHELL_PID=${LAUNCHED_PID}
sleep 1
kill -0 "${APPIMAGE_SHELL_PID}"
test "$(query_status | status_generation)" = "${FIRST_GENERATION}"

launch_shell env APPIMAGE_EXTRACT_AND_RUN=1 "${APPIMAGE}"
require_relaunch_exit "${LAUNCHED_PID}"
test "$(query_status | status_generation)" = "${FIRST_GENERATION}"

sudo dpkg -i "${DEB_PACKAGE}"
PACKAGE_INSTALLED=1
grep -Fx 'native-shell-uninstall-preservation' "${SAGE_SMOKE_HOME}/preserve.sentinel"
test "$(query_status | status_generation)" = "${FIRST_GENERATION}"

echo "native-shell Linux installed runtime smoke passed generation=${FIRST_GENERATION}"
