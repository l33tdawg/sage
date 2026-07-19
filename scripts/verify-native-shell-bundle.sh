#!/usr/bin/env bash
# Inspect a completed Tauri package and prove it contains exactly one bundled
# sage-gui executable whose embedded version matches the shell package version.

set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <rust-target-triple> <bundle-root> <expected-version>" >&2
  exit 2
fi

TARGET_TRIPLE=$1
BUNDLE_ROOT=$2
EXPECTED_VERSION=$3

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

if command -v sha256sum >/dev/null 2>&1; then
  DAEMON_SHA256=$(sha256sum "${DAEMON_PATH}" | awk '{print $1}')
else
  DAEMON_SHA256=$(shasum -a 256 "${DAEMON_PATH}" | awk '{print $1}')
fi
echo "verified bundled ${DAEMON_NAME} version=${EXPECTED_VERSION} sha256=${DAEMON_SHA256}"
