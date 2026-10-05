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

compute_source_state() {
  local snapshot_sha cleanliness
  snapshot_sha=$(
    {
      git -C "${ROOT}" diff --binary HEAD
      git -C "${ROOT}" ls-files --others --exclude-standard | while IFS= read -r candidate; do
        printf 'untracked=%s\n' "${candidate}"
        /usr/bin/shasum -a 256 "${ROOT}/${candidate}"
      done
    } | /usr/bin/shasum -a 256 | awk '{print $1}'
  ) || return 1
  cleanliness=clean
  if [ -n "$(git -C "${ROOT}" status --porcelain=v1 --untracked-files=all)" ]; then
    cleanliness=dirty
  fi
  printf '%s:%s\n' "${cleanliness}" "${snapshot_sha}"
}

build_probe() {
  mkdir -p "${TOOL_DIR}"
  if [ ! -x "${TOOL}" ] || [ "${SOURCE}" -nt "${TOOL}" ]; then
    SWIFT_MODULECACHE_PATH="${TOOL_DIR}/module-cache" \
    CLANG_MODULE_CACHE_PATH="${TOOL_DIR}/clang-cache" \
      xcrun swiftc "${SOURCE}" -o "${TOOL}" \
        -framework AppKit -framework ApplicationServices
  fi
}

validate_result() {
  node - "$1" "$2" <<'JS'
const fs = require('node:fs');
const assert = require('node:assert/strict');
const result = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const scenario = process.argv[3];
assert.ok(['retry-fail', 'retry-restore', 'brain-menu-focus'].includes(scenario));
assert.equal(result.scenario, scenario);
assert.equal(result.passed, true);
assert.equal(result.trusted, true);
assert.equal(result.system_ax_server, true);
assert.equal(result.voiceover_spoken_evidence, false);
assert.equal(result.bundle_id, 'com.sage.cerebrum.beta');
assert.ok(Number.isInteger(result.pid) && result.pid > 1);
assert.equal(result.traversal_limits.maximum_nodes, 8192);
if (scenario === 'brain-menu-focus') {
  assert.equal(result.schema, 'sage.v12.native-system-ax.brain.v1');
  for (const field of ['memory_selection_preserved', 'agent_selection_preserved', 'exact_application_and_system_focus', 'synthetic_windowserver_keyboard_events']) assert.equal(result[field], true);
  assert.equal(result.physical_keyboard_event_routing, false);
  assert.equal(result.fixture_focus_injection, false);
  assert.deepEqual(result.keyboard_sequence, ['Down', 'Control-Command-I', 'Down']);
  assert.deepEqual(result.menu_actions.map(action => action.path), [
    ['Navigate', 'Brain'], ['View', 'Brain Presentation', 'List View'],
    ['View', 'Hide Inspector'], ['View', 'Show Inspector'], ['View', 'Hide Inspector'],
    ['View', 'Brain Mode', 'Agent Network'], ['View', 'Show Inspector'],
  ]);
  for (const action of result.menu_actions) {
    assert.equal(action.ax_press_results.length, action.path.length);
    assert.ok(action.ax_press_results.every(code => code === 0 || code === -25204));
  }
  assert.equal(result.initial_table.identifier, 'brain-memory-table');
  assert.ok(['AXTable', 'AXOutline'].includes(result.initial_table.role));
  assert.ok(Number.isInteger(result.initial_table.row_count) && result.initial_table.row_count > 0);
  assert.equal(result.inspector_close.identifier, 'brain-inspector-close');
  assert.equal(result.inspector_close.role, 'AXButton');
  assert.equal(result.final.identifier, 'brain-connectome-table');
  assert.ok(['AXTable', 'AXOutline'].includes(result.final.role));
  assert.equal(result.final.row_count, 3);
  assert.equal(result.focused_identifier, result.final.identifier);
} else {
  assert.equal(result.schema, 'sage.v12.native-system-ax.v1');
  assert.equal(result.ready.identifier, 'brain-metal-retry');
  assert.equal(result.ready.role, 'AXButton');
  assert.equal(result.in_flight.enabled, false);
  assert.equal(result.in_flight.value, 'In progress');
  assert.equal(result.final.identifier, scenario === 'retry-fail' ? 'brain-metal-retry' : 'brain-memory-metal-surface');
  assert.equal(result.focused_identifier, result.final.identifier);
}
JS
}

usage() {
  cat >&2 <<'EOF'
usage:
  scripts/v12-native-system-ax.sh --preflight [--prompt]
  scripts/v12-native-system-ax.sh --scenario <retry-fail|retry-restore|brain-menu-focus> --evidence <directory>
EOF
  exit 64
}

MODE=""
PROMPT=0
SCENARIO=""
EVIDENCE_DIR=""
VALIDATE_RESULT=""
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
    --validate-result)
      shift
      [ "$#" -gt 0 ] || usage
      VALIDATE_RESULT=$1
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

if [ -n "${VALIDATE_RESULT}" ]; then
  validate_result "${VALIDATE_RESULT}" "${SCENARIO}"
  exit 0
fi

build_probe

if [ "${MODE}" = preflight ]; then
  if [ "${PROMPT}" -eq 1 ]; then
    exec "${TOOL}" --preflight --prompt
  fi
  exec "${TOOL}" --preflight
fi

[ "${MODE}" = scenario ] || usage
case "${SCENARIO}" in retry-fail|retry-restore|brain-menu-focus) ;; *) usage ;; esac
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
COMMIT=$(git -C "${ROOT}" rev-parse HEAD)
SOURCE_STATE=$(compute_source_state)
BUILD_DIR="${ROOT}/dist/v12-native/ax-debug/${VERSION}-$$"
SAGE_NATIVE_VERSION="${VERSION}" \
SAGE_NATIVE_CONFIGURATION=debug \
SAGE_NATIVE_OUTPUT_DIR="${BUILD_DIR}" \
SAGE_NATIVE_SCRATCH_PATH="${BUILD_DIR}/swiftpm" \
  bash "${ROOT}/scripts/build-native-cerebrum-macos.sh" >/dev/null

if [ "$(compute_source_state)" != "${SOURCE_STATE}" ]; then
  echo "source state changed during system AX app build" >&2
  exit 1
fi

APP_PATH="${BUILD_DIR}/SAGE CEREBRUM Native.app"
EXECUTABLE="${APP_PATH}/Contents/MacOS/SAGECerebrumNative"
test -x "${EXECUTABLE}"

mkdir -p "${EVIDENCE_DIR}"
EVIDENCE_DIR=$(cd "${EVIDENCE_DIR}" && pwd -P)
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${SCENARIO}-$$"
RUN_DIR="${EVIDENCE_DIR}/${RUN_ID}"
mkdir -p "${RUN_DIR}"
APP_LOG="${RUN_DIR}/app.log"
PROBE_LOG="${RUN_DIR}/probe.log"
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
if [ "${SCENARIO}" = brain-menu-focus ]; then
  SAGE_NATIVE_DESIGN_PREVIEW=1 \
  SAGE_NATIVE_PREVIEW_ROUTE=overview \
    "${EXECUTABLE}" >"${APP_LOG}" 2>&1 &
else
  SAGE_NATIVE_DESIGN_PREVIEW=1 \
  SAGE_NATIVE_PREVIEW_ROUTE=brain \
  SAGE_NATIVE_AX_METAL=unavailable \
  SAGE_NATIVE_AX_RETRY_RESULT="${RETRY_RESULT}" \
  SAGE_NATIVE_AX_RETRY_DELAY_MS=900 \
    "${EXECUTABLE}" >"${APP_LOG}" 2>&1 &
fi
APP_PID=$!

set +e
"${TOOL}" --pid "${APP_PID}" --scenario "${SCENARIO}" --timeout 45 >"${RESULT}" 2>"${PROBE_LOG}"
probe_status=$?
set -e
cat "${PROBE_LOG}" >&2
[ "${probe_status}" -eq 0 ] || exit "${probe_status}"
validate_result "${RESULT}" "${SCENARIO}"
if [ "$(compute_source_state)" != "${SOURCE_STATE}" ]; then
  echo "source state changed during system AX app execution" >&2
  exit 1
fi
{
  printf 'schema=sage.v12.native-system-ax.manifest.v1\n'
  printf 'run_id=%s\n' "${RUN_ID}"
  printf 'scenario=%s\n' "${SCENARIO}"
  printf 'commit=%s\n' "${COMMIT}"
  printf 'source_state=%s\n' "${SOURCE_STATE}"
  printf 'bundle_id=%s\n' "$(plutil -extract CFBundleIdentifier raw "${APP_PATH}/Contents/Info.plist")"
  printf 'bundle_version=%s\n' "$(plutil -extract SAGEBetaVersion raw "${APP_PATH}/Contents/Info.plist")"
  printf 'architecture=%s\n' "$(uname -m)"
  sw_vers
  system_profiler SPDisplaysDataType
  /usr/bin/shasum -a 256 "${TOOL}" "${EXECUTABLE}" "${RESULT}" "${APP_LOG}" "${PROBE_LOG}"
} >"${MANIFEST}"
/usr/bin/shasum -a 256 "${RESULT}" "${APP_LOG}" "${PROBE_LOG}" "${MANIFEST}" >"${RUN_DIR}/SHA256SUMS"
printf '%s\n' "${RESULT}"
