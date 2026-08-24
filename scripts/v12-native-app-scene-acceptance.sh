#!/usr/bin/env bash
set -euo pipefail

if [ "$(uname -s)" != "Darwin" ]; then
  echo "v12 native app-scene acceptance requires macOS" >&2
  exit 1
fi

ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
VERSION=${SAGE_NATIVE_VERSION:-12.0.0-beta.1}
COMMIT=$(git -C "${ROOT}" rev-parse HEAD)
BUILD_DIR="${ROOT}/dist/v12-native/app-scene-debug/${VERSION}-$$"
EVIDENCE_DIR="${SAGE_NATIVE_APP_SCENE_EVIDENCE_DIR:-${ROOT}/dist/v12-native/${VERSION}/app-scene-validation}"

compute_source_state() {
  local snapshot_sha cleanliness
  snapshot_sha=$(
    {
      git -C "${ROOT}" diff --binary HEAD
      git -C "${ROOT}" ls-files --others --exclude-standard | while IFS= read -r candidate; do
        printf 'untracked=%s\n' "${candidate}"
        shasum -a 256 "${ROOT}/${candidate}"
      done
    } | shasum -a 256 | awk '{print $1}'
  )
  cleanliness=clean
  if [ -n "$(git -C "${ROOT}" status --porcelain=v1 --untracked-files=all)" ]; then
    cleanliness=dirty
  fi
  printf '%s:%s\n' "${cleanliness}" "${snapshot_sha}"
}

mkdir -p "${EVIDENCE_DIR}"
EVIDENCE_DIR=$(cd "${EVIDENCE_DIR}" && pwd -P)
printf '%s\n' 'app-scene acceptance pending' >"${EVIDENCE_DIR}/STATUS.txt"
SOURCE_STATE_BEFORE_BUILD=$(compute_source_state)

SAGE_NATIVE_VERSION="${VERSION}" \
SAGE_NATIVE_CONFIGURATION=debug \
SAGE_NATIVE_OUTPUT_DIR="${BUILD_DIR}" \
SAGE_NATIVE_SCRATCH_PATH="${BUILD_DIR}/swiftpm" \
  bash "${ROOT}/scripts/build-native-cerebrum-macos.sh" >/dev/null

SOURCE_STATE_AFTER_BUILD=$(compute_source_state)
if [ "${SOURCE_STATE_AFTER_BUILD}" != "${SOURCE_STATE_BEFORE_BUILD}" ]; then
  echo "source state changed during native app-scene build" >&2
  exit 1
fi
SOURCE_STATE="${SOURCE_STATE_BEFORE_BUILD}"

APP_PATH="${BUILD_DIR}/SAGE CEREBRUM Native.app"
EXECUTABLE="${APP_PATH}/Contents/MacOS/SAGECerebrumNative"
test -x "${EXECUTABLE}"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-app-scene-$$"
RUN_DIR="${EVIDENCE_DIR}/${RUN_ID}"
mkdir "${RUN_DIR}"
RESULT="${RUN_DIR}/app-scene.json"
APP_LOG="${RUN_DIR}/app.log"
MANIFEST="${RUN_DIR}/manifest.txt"
APP_PID=""
LAUNCHED_PID=""

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

SAGE_NATIVE_DESIGN_PREVIEW=1 \
SAGE_NATIVE_PREVIEW_ROUTE=overview \
SAGE_NATIVE_APP_SCENE_ACCEPTANCE=rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle \
SAGE_NATIVE_APP_SCENE_COMMIT="${COMMIT}" \
SAGE_NATIVE_APP_SCENE_SOURCE_STATE="${SOURCE_STATE}" \
SAGE_NATIVE_APP_SCENE_RUN_ID="${RUN_ID}" \
  "${EXECUTABLE}" >"${RESULT}" 2>"${APP_LOG}" &
APP_PID=$!
LAUNCHED_PID="${APP_PID}"

deadline=$((SECONDS + 40))
while kill -0 "${APP_PID}" 2>/dev/null && [ "${SECONDS}" -lt "${deadline}" ]; do
  sleep 0.1
done
if kill -0 "${APP_PID}" 2>/dev/null; then
  echo "native app-scene fixture exceeded its 40-second outer deadline" >&2
  exit 1
fi
set +e
wait "${APP_PID}"
status=$?
set -e
APP_PID=""
[ "${status}" -eq 0 ] || {
  echo "native app-scene fixture failed with status ${status}; see ${APP_LOG}" >&2
  exit "${status}"
}

node "${ROOT}/scripts/v12-native-app-scene-validate.mjs" "${RESULT}" "${COMMIT}" "${SOURCE_STATE}" "${RUN_ID}" "${LAUNCHED_PID}"

{
  printf 'schema=sage.v12.native-app-scene.manifest.v4\n'
  printf 'run_id=%s\n' "${RUN_ID}"
  printf 'scenario=rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle\n'
  printf 'commit=%s\n' "${COMMIT}"
  printf 'source_state=%s\n' "${SOURCE_STATE}"
  printf 'bundle_id=%s\n' "$(plutil -extract CFBundleIdentifier raw "${APP_PATH}/Contents/Info.plist")"
  printf 'bundle_version=%s\n' "$(plutil -extract SAGEBetaVersion raw "${APP_PATH}/Contents/Info.plist")"
  printf 'architecture=%s\n' "$(uname -m)"
  sw_vers
  shasum -a 256 "${EXECUTABLE}" "${RESULT}" "${APP_LOG}"
} >"${MANIFEST}"
shasum -a 256 "${RESULT}" "${APP_LOG}" "${MANIFEST}" >"${RUN_DIR}/SHA256SUMS"
STATUS_TMP="${EVIDENCE_DIR}/.STATUS.txt.$$"
printf '%s\n' 'app-scene acceptance passed' >"${STATUS_TMP}"
mv "${STATUS_TMP}" "${EVIDENCE_DIR}/STATUS.txt"
printf '%s\n' "${RESULT}"
