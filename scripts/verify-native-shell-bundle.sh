#!/usr/bin/env bash
# Inspect a completed Tauri package and prove it contains exactly one bundled
# sage-gui executable whose embedded version matches the shell build version.

set -euo pipefail

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 <rust-target-triple> <bundle-root> <expected-version> [evidence-json]" >&2
  exit 2
fi

TARGET_TRIPLE=$1
BUNDLE_ROOT=$2
EXPECTED_VERSION=$3
EVIDENCE_JSON=${4:-}

if [ ! -d "${BUNDLE_ROOT}" ]; then
  echo "native-shell bundle root does not exist: ${BUNDLE_ROOT}" >&2
  exit 1
fi

EXTRACT_ROOT=
cleanup() {
  if [ -n "${EXTRACT_ROOT}" ] && [ -d "${EXTRACT_ROOT}" ]; then
    rm -rf -- "${EXTRACT_ROOT}"
  fi
}
trap cleanup EXIT INT TERM

case "${TARGET_TRIPLE}" in
  *apple-darwin)
    SEARCH_ROOT=${BUNDLE_ROOT}/macos
    DAEMON_NAME=sage-gui
    DAEMON_PATTERN='*/Contents/Resources/binaries/sage-gui'
    APP_COUNT=$(find "${SEARCH_ROOT}" -maxdepth 1 -type d -name '*.app' 2>/dev/null | wc -l | tr -d ' ')
    if [ "${APP_COUNT}" -ne 1 ]; then
      echo "native-shell bundle must contain exactly one macOS app; found ${APP_COUNT}" >&2
      exit 1
    fi
    APP_PATH=$(find "${SEARCH_ROOT}" -maxdepth 1 -type d -name '*.app' | head -1)
    SHELL_ARTIFACT_COUNT=$(find "${APP_PATH}/Contents/MacOS" -maxdepth 1 -type f | wc -l | tr -d ' ')
    if [ "${SHELL_ARTIFACT_COUNT}" -ne 1 ]; then
      echo "native-shell macOS app must contain exactly one shell executable; found ${SHELL_ARTIFACT_COUNT}" >&2
      exit 1
    fi
    SHELL_ARTIFACT=$(find "${APP_PATH}/Contents/MacOS" -maxdepth 1 -type f | head -1)
    SHELL_ARTIFACT_KIND=app-executable
    ;;
  *unknown-linux-gnu)
    command -v dpkg-deb >/dev/null
    PACKAGE=$(find "${BUNDLE_ROOT}/deb" -maxdepth 1 -type f -name '*.deb' | head -1)
    if [ -z "${PACKAGE}" ]; then
      echo "native-shell Debian package is missing" >&2
      exit 1
    fi
    EXTRACT_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-deb.XXXXXX")
    dpkg-deb -x "${PACKAGE}" "${EXTRACT_ROOT}"
    SEARCH_ROOT=${EXTRACT_ROOT}
    DAEMON_NAME=sage-gui
    DAEMON_PATTERN='*/binaries/sage-gui'
    SHELL_ARTIFACT=${PACKAGE}
    SHELL_ARTIFACT_KIND=deb
    ;;
  *pc-windows-msvc)
    command -v 7z >/dev/null
    PACKAGE=$(find "${BUNDLE_ROOT}/nsis" -maxdepth 1 -type f -iname '*setup*.exe' | head -1)
    if [ -z "${PACKAGE}" ]; then
      echo "native-shell NSIS package is missing" >&2
      exit 1
    fi
    EXTRACT_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-nsis.XXXXXX")
    7z x -y "-o${EXTRACT_ROOT}" "${PACKAGE}" >/dev/null
    SEARCH_ROOT=${EXTRACT_ROOT}
    DAEMON_NAME=sage-gui.exe
    DAEMON_PATTERN='*/binaries/sage-gui.exe'
    SHELL_ARTIFACT=${PACKAGE}
    SHELL_ARTIFACT_KIND=nsis
    ;;
  *)
    echo "unsupported native-shell bundle target: ${TARGET_TRIPLE}" >&2
    exit 2
    ;;
esac

if [ ! -d "${SEARCH_ROOT}" ]; then
  echo "native-shell package content is missing: ${SEARCH_ROOT}" >&2
  exit 1
fi

DAEMON_COUNT=$(find "${SEARCH_ROOT}" -type f -path "${DAEMON_PATTERN}" | wc -l | tr -d ' ')
if [ "${DAEMON_COUNT}" -ne 1 ]; then
  echo "native-shell package must contain exactly one ${DAEMON_NAME}; found ${DAEMON_COUNT}" >&2
  exit 1
fi
DAEMON_PATH=$(find "${SEARCH_ROOT}" -type f -path "${DAEMON_PATTERN}" | head -1)

VERSION_OUTPUT=$("${DAEMON_PATH}" version)
case "${VERSION_OUTPUT}" in
  "sage-gui ${EXPECTED_VERSION} "*) ;;
  *)
    echo "bundled daemon version mismatch: expected ${EXPECTED_VERSION}, got ${VERSION_OUTPUT}" >&2
    exit 1
    ;;
esac

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

json_escape() {
  case "$1" in
    *$'\n'*|*$'\r'*)
      echo "native-shell evidence fields must not contain newlines" >&2
      return 1
      ;;
  esac
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

DAEMON_SHA256=$(sha256_file "${DAEMON_PATH}")
SHELL_ARTIFACT_SHA256=$(sha256_file "${SHELL_ARTIFACT}")
DAEMON_SIZE=$(wc -c < "${DAEMON_PATH}" | tr -d ' ')
SHELL_ARTIFACT_SIZE=$(wc -c < "${SHELL_ARTIFACT}" | tr -d ' ')
DAEMON_PACKAGE_PATH=${DAEMON_PATH#"${SEARCH_ROOT}"/}
SHELL_ARTIFACT_PATH=${SHELL_ARTIFACT#"${BUNDLE_ROOT}"/}

if [ -n "${EVIDENCE_JSON}" ]; then
  EVIDENCE_PARENT=$(dirname "${EVIDENCE_JSON}")
  if [ ! -d "${EVIDENCE_PARENT}" ]; then
    echo "native-shell evidence parent does not exist: ${EVIDENCE_PARENT}" >&2
    exit 1
  fi
  EVIDENCE_TMP=${EVIDENCE_JSON}.tmp.$$
  trap 'rm -f -- "${EVIDENCE_TMP:-}"; cleanup' EXIT INT TERM
  {
    printf '{\n'
    printf '  "schema": "dev.sage.native-shell.release-pair/v1",\n'
    printf '  "target": "%s",\n' "$(json_escape "${TARGET_TRIPLE}")"
    printf '  "build_version": "%s",\n' "$(json_escape "${EXPECTED_VERSION}")"
    printf '  "shell_artifact": {\n'
    printf '    "kind": "%s",\n' "$(json_escape "${SHELL_ARTIFACT_KIND}")"
    printf '    "path": "%s",\n' "$(json_escape "${SHELL_ARTIFACT_PATH}")"
    printf '    "size_bytes": %s,\n' "${SHELL_ARTIFACT_SIZE}"
    printf '    "sha256": "%s"\n' "${SHELL_ARTIFACT_SHA256}"
    printf '  },\n'
    printf '  "bundled_daemon": {\n'
    printf '    "name": "%s",\n' "$(json_escape "${DAEMON_NAME}")"
    printf '    "package_path": "%s",\n' "$(json_escape "${DAEMON_PACKAGE_PATH}")"
    printf '    "size_bytes": %s,\n' "${DAEMON_SIZE}"
    printf '    "sha256": "%s"\n' "${DAEMON_SHA256}"
    printf '  }\n'
    printf '}\n'
  } > "${EVIDENCE_TMP}"
  mv -- "${EVIDENCE_TMP}" "${EVIDENCE_JSON}"
fi
echo "verified bundled ${DAEMON_NAME} version=${EXPECTED_VERSION} sha256=${DAEMON_SHA256}"
