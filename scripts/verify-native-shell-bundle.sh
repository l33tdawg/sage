#!/usr/bin/env bash
# Inspect a completed Tauri package and prove it contains exactly one bundled
# sage-gui executable whose embedded version matches the shell build version.

set -euo pipefail

if [ "$#" -lt 3 ] || [ "$#" -gt 5 ]; then
  echo "usage: $0 <rust-target-triple> <bundle-root> <expected-version> [evidence-json] [package-kind]" >&2
  exit 2
fi

TARGET_TRIPLE=$1
BUNDLE_ROOT=$2
EXPECTED_VERSION=$3
EVIDENCE_JSON=${4:-}
PACKAGE_KIND=${5:-}

if [ ! -d "${BUNDLE_ROOT}" ]; then
  echo "native-shell bundle root does not exist: ${BUNDLE_ROOT}" >&2
  exit 1
fi
BUNDLE_ROOT=$(cd "${BUNDLE_ROOT}" && pwd -P)

EXTRACT_ROOT=
cleanup() {
  if [ -n "${EXTRACT_ROOT}" ] && [ -d "${EXTRACT_ROOT}" ]; then
    rm -rf -- "${EXTRACT_ROOT}"
  fi
}
trap cleanup EXIT INT TERM

case "${TARGET_TRIPLE}" in
  aarch64-apple-darwin)
    if [ -n "${PACKAGE_KIND}" ] && [ "${PACKAGE_KIND}" != app ]; then
      echo "unsupported package kind for ${TARGET_TRIPLE}: ${PACKAGE_KIND}" >&2
      exit 2
    fi
    EXPECTED_GOOS=darwin
    EXPECTED_GOARCH=arm64
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
  x86_64-unknown-linux-gnu)
    EXPECTED_GOOS=linux
    EXPECTED_GOARCH=amd64
    DAEMON_NAME=sage-gui
    DAEMON_PATTERN='*/binaries/sage-gui'
    case "${PACKAGE_KIND:-deb}" in
      deb)
        command -v dpkg-deb >/dev/null
        PACKAGE_COUNT=$(find "${BUNDLE_ROOT}/deb" -maxdepth 1 -type f -name '*.deb' 2>/dev/null | wc -l | tr -d ' ')
        if [ "${PACKAGE_COUNT}" -ne 1 ]; then
          echo "native-shell bundle must contain exactly one Debian package; found ${PACKAGE_COUNT}" >&2
          exit 1
        fi
        PACKAGE=$(find "${BUNDLE_ROOT}/deb" -maxdepth 1 -type f -name '*.deb' | head -1)
        EXTRACT_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-deb.XXXXXX")
        dpkg-deb -x "${PACKAGE}" "${EXTRACT_ROOT}"
        SEARCH_ROOT=${EXTRACT_ROOT}
        SHELL_ARTIFACT=${PACKAGE}
        SHELL_ARTIFACT_KIND=deb
        ;;
      appimage)
        PACKAGE_COUNT=$(find "${BUNDLE_ROOT}/appimage" -maxdepth 1 -type f -name '*.AppImage' 2>/dev/null | wc -l | tr -d ' ')
        if [ "${PACKAGE_COUNT}" -ne 1 ]; then
          echo "native-shell bundle must contain exactly one AppImage; found ${PACKAGE_COUNT}" >&2
          exit 1
        fi
        PACKAGE=$(find "${BUNDLE_ROOT}/appimage" -maxdepth 1 -type f -name '*.AppImage' | head -1)
        if [ ! -x "${PACKAGE}" ]; then
          echo "native-shell AppImage is not executable: ${PACKAGE}" >&2
          exit 1
        fi
        EXTRACT_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-appimage.XXXXXX")
        (
          cd "${EXTRACT_ROOT}"
          "${PACKAGE}" --appimage-extract >/dev/null
        )
        SEARCH_ROOT=${EXTRACT_ROOT}/squashfs-root
        SHELL_ARTIFACT=${PACKAGE}
        SHELL_ARTIFACT_KIND=appimage
        ;;
      *)
        echo "unsupported package kind for ${TARGET_TRIPLE}: ${PACKAGE_KIND}" >&2
        exit 2
        ;;
    esac
    ;;
  x86_64-pc-windows-msvc)
    if [ -n "${PACKAGE_KIND}" ] && [ "${PACKAGE_KIND}" != nsis ]; then
      echo "unsupported package kind for ${TARGET_TRIPLE}: ${PACKAGE_KIND}" >&2
      exit 2
    fi
    EXPECTED_GOOS=windows
    EXPECTED_GOARCH=amd64
    command -v 7z >/dev/null
    PACKAGE_COUNT=$(find "${BUNDLE_ROOT}/nsis" -maxdepth 1 -type f -iname '*setup*.exe' 2>/dev/null | wc -l | tr -d ' ')
    if [ "${PACKAGE_COUNT}" -ne 1 ]; then
      echo "native-shell bundle must contain exactly one NSIS package; found ${PACKAGE_COUNT}" >&2
      exit 1
    fi
    PACKAGE=$(find "${BUNDLE_ROOT}/nsis" -maxdepth 1 -type f -iname '*setup*.exe' | head -1)
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

BUILD_INFO=
if BUILD_INFO=$(go version -m "${DAEMON_PATH}" 2>/dev/null); then
  BUILD_GOOS=$(printf '%s\n' "${BUILD_INFO}" | awk '$1 == "build" && $2 ~ /^GOOS=/ { sub(/^GOOS=/, "", $2); print $2; exit }')
  BUILD_GOARCH=$(printf '%s\n' "${BUILD_INFO}" | awk '$1 == "build" && $2 ~ /^GOARCH=/ { sub(/^GOARCH=/, "", $2); print $2; exit }')
  BUILD_VERSION=$(printf '%s\n' "${BUILD_INFO}" | sed -n 's/.*-X main\.version=\([^ "\t]*\).*/\1/p' | head -1)
  if [ "${BUILD_GOOS}" != "${EXPECTED_GOOS}" ] || [ "${BUILD_GOARCH}" != "${EXPECTED_GOARCH}" ]; then
    echo "bundled daemon target mismatch: expected ${EXPECTED_GOOS}/${EXPECTED_GOARCH}, got ${BUILD_GOOS:-unknown}/${BUILD_GOARCH:-unknown}" >&2
    exit 1
  fi
  if [ "${BUILD_VERSION}" != "${EXPECTED_VERSION}" ]; then
    echo "bundled daemon metadata version mismatch: expected ${EXPECTED_VERSION}, got ${BUILD_VERSION:-unknown}" >&2
    exit 1
  fi
fi

if [ -z "${BUILD_INFO}" ]; then
  VERSION_OUTPUT=$("${DAEMON_PATH}" version)
  case "${VERSION_OUTPUT}" in
    "sage-gui ${EXPECTED_VERSION} "*) ;;
    *)
      echo "bundled daemon version mismatch: expected ${EXPECTED_VERSION}, got ${VERSION_OUTPUT}" >&2
      exit 1
      ;;
  esac
fi

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
