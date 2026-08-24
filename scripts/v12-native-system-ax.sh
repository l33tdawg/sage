#!/usr/bin/env bash
set -euo pipefail

if [ "$(uname -s)" != "Darwin" ]; then
  echo "v12 native system AX acceptance requires macOS" >&2
  exit 1
fi

ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
SOURCE="${ROOT}/scripts/v12-native-system-ax.swift"
TOOL_DIR="${SAGE_AX_TOOL_DIR:-${ROOT}/dist/v12-native/ax-tools}"
TOOL="${TOOL_DIR}/v12-native-system-ax"

build_probe() {
  mkdir -p "${TOOL_DIR}"
  if [ ! -x "${TOOL}" ] || [ "${SOURCE}" -nt "${TOOL}" ]; then
    SWIFT_MODULECACHE_PATH="${TOOL_DIR}/module-cache" \
    CLANG_MODULE_CACHE_PATH="${TOOL_DIR}/clang-cache" \
      xcrun swiftc "${SOURCE}" -o "${TOOL}" \
        -framework AppKit -framework ApplicationServices
  fi
}

usage() {
  cat >&2 <<'EOF'
usage:
  scripts/v12-native-system-ax.sh --preflight [--prompt]
  scripts/v12-native-system-ax.sh --scenario <retry-fail|retry-restore> --evidence <directory>
EOF
  exit 64
}

MODE=""
PROMPT=0
SCENARIO=""
EVIDENCE_DIR=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --preflight) MODE=preflight ;;
    --prompt) PROMPT=1 ;;
    --scenario)
      shift
      [ "$#" -gt 0 ] || usage
      MODE=scenario
      SCENARIO=$1
      ;;
    --evidence)
      shift
      [ "$#" -gt 0 ] || usage
      EVIDENCE_DIR=$1
      ;;
    *) usage ;;
  esac
  shift
done

build_probe

if [ "${MODE}" = preflight ]; then
  if [ "${PROMPT}" -eq 1 ]; then
    exec "${TOOL}" --preflight --prompt
  fi
  exec "${TOOL}" --preflight
fi

[ "${MODE}" = scenario ] || usage
case "${SCENARIO}" in retry-fail|retry-restore) ;; *) usage ;; esac
[ -n "${EVIDENCE_DIR}" ] || usage

set +e
"${TOOL}" --preflight
status=$?
set -e
if [ "${status}" -ne 0 ]; then
  if [ "${status}" -eq 77 ]; then
    echo "grant Accessibility access to ${TOOL}, then rerun this scenario" >&2
  fi
  exit "${status}"
fi

VERSION=${SAGE_NATIVE_VERSION:-12.0.0-beta.1}
BUILD_DIR="${ROOT}/dist/v12-native/ax-debug/${VERSION}"
SAGE_NATIVE_VERSION="${VERSION}" \
SAGE_NATIVE_CONFIGURATION=debug \
SAGE_NATIVE_OUTPUT_DIR="${BUILD_DIR}" \
  bash "${ROOT}/scripts/build-native-cerebrum-macos.sh" >/dev/null

APP_PATH="${BUILD_DIR}/SAGE CEREBRUM Native.app"
EXECUTABLE="${APP_PATH}/Contents/MacOS/SAGECerebrumNative"
test -x "${EXECUTABLE}"

mkdir -p "${EVIDENCE_DIR}"
EVIDENCE_DIR=$(cd "${EVIDENCE_DIR}" && pwd -P)
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${SCENARIO}"
RUN_DIR="${EVIDENCE_DIR}/${RUN_ID}"
mkdir -p "${RUN_DIR}"
APP_LOG="${RUN_DIR}/app.log"
RESULT="${RUN_DIR}/system-ax.json"
MANIFEST="${RUN_DIR}/manifest.txt"
APP_PID=""

cleanup() {
  original_status=$?
  trap - EXIT INT TERM
  if [ -n "${APP_PID}" ] && kill -0 "${APP_PID}" 2>/dev/null; then
    actual=$(ps -p "${APP_PID}" -o comm= 2>/dev/null || true)
    if [ "${actual}" = "${EXECUTABLE}" ]; then
      kill -TERM "${APP_PID}" 2>/dev/null || true
      deadline=$((SECONDS + 10))
      while [ "${SECONDS}" -lt "${deadline}" ] && kill -0 "${APP_PID}" 2>/dev/null; do
        sleep 0.1
      done
      if kill -0 "${APP_PID}" 2>/dev/null; then
        kill -KILL "${APP_PID}" 2>/dev/null || true
      fi
      wait "${APP_PID}" 2>/dev/null || true
    else
      echo "refusing to signal pid ${APP_PID}: expected ${EXECUTABLE}, found ${actual}" >&2
      original_status=1
    fi
  fi
  exit "${original_status}"
}
on_signal() { exit 130; }
trap cleanup EXIT
trap on_signal INT TERM

RETRY_RESULT=fail
if [ "${SCENARIO}" = retry-restore ]; then RETRY_RESULT=restore; fi
SAGE_NATIVE_DESIGN_PREVIEW=1 \
SAGE_NATIVE_PREVIEW_ROUTE=brain \
SAGE_NATIVE_AX_METAL=unavailable \
SAGE_NATIVE_AX_RETRY_RESULT="${RETRY_RESULT}" \
SAGE_NATIVE_AX_RETRY_DELAY_MS=900 \
  "${EXECUTABLE}" >"${APP_LOG}" 2>&1 &
APP_PID=$!

"${TOOL}" --pid "${APP_PID}" --scenario "${SCENARIO}" --timeout 20 >"${RESULT}"
grep -Fq '"passed":true' "${RESULT}"
grep -Fq '"system_ax_server":true' "${RESULT}"
grep -Fq '"voiceover_spoken_evidence":false' "${RESULT}"
{
  printf 'schema=sage.v12.native-system-ax.manifest.v1\n'
  printf 'run_id=%s\n' "${RUN_ID}"
  printf 'scenario=%s\n' "${SCENARIO}"
  printf 'commit=%s\n' "$(git -C "${ROOT}" rev-parse HEAD)"
  printf 'bundle_id=%s\n' "$(plutil -extract CFBundleIdentifier raw "${APP_PATH}/Contents/Info.plist")"
  printf 'bundle_version=%s\n' "$(plutil -extract SAGEBetaVersion raw "${APP_PATH}/Contents/Info.plist")"
  printf 'architecture=%s\n' "$(uname -m)"
  sw_vers
  system_profiler SPDisplaysDataType
  shasum -a 256 "${TOOL}" "${EXECUTABLE}" "${RESULT}" "${APP_LOG}"
} >"${MANIFEST}"
shasum -a 256 "${RESULT}" "${APP_LOG}" "${MANIFEST}" >"${RUN_DIR}/SHA256SUMS"
printf '%s\n' "${RESULT}"
