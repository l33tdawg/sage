#!/usr/bin/env bash
# Build the same-platform sage-gui daemon into the Tauri resource tree. Tauri
# packages that file with the shell; it is never fetched or resolved from PATH
# at runtime.

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <rust-target-triple>" >&2
  exit 2
fi

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
TARGET_TRIPLE=$1
case "${TARGET_TRIPLE}" in
  aarch64-apple-darwin)
    GO_OS=darwin
    GO_ARCH=arm64
    DAEMON_NAME=sage-gui
    ;;
  x86_64-pc-windows-msvc)
    GO_OS=windows
    GO_ARCH=amd64
    DAEMON_NAME=sage-gui.exe
    ;;
  x86_64-unknown-linux-gnu)
    GO_OS=linux
    GO_ARCH=amd64
    DAEMON_NAME=sage-gui
    ;;
  *)
    echo "unsupported native-shell target triple: ${TARGET_TRIPLE}" >&2
    exit 2
    ;;
esac

VERSION=${SAGE_DAEMON_VERSION:-$(git -C "${REPO_ROOT}" describe --tags --always)}
COMMIT=$(git -C "${REPO_ROOT}" rev-parse --short HEAD)
BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
OUTPUT_DIR=${REPO_ROOT}/desktop/sage-shell/binaries

VERSION_CORE=${VERSION#v}
SEMVER_PATTERN='^11\.(10|11|12|13)\.[0-9]+(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'
if [[ ! "${VERSION_CORE}" =~ ${SEMVER_PATTERN} ]]; then
  echo "SAGE_DAEMON_VERSION must be an SSCP-compatible v11.10.x through v11.13.x semver, got: ${VERSION}" >&2
  exit 2
fi
if [ "${OUTPUT_DIR}" != "${REPO_ROOT}/desktop/sage-shell/binaries" ]; then
  echo "refusing to stage daemon outside the native-shell resource directory" >&2
  exit 2
fi

rm -rf -- "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"
env CGO_ENABLED=0 GOOS="${GO_OS}" GOARCH="${GO_ARCH}" \
  GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/sage-native-shell-gocache}" go build \
  -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${BUILD_DATE}" \
  -o "${OUTPUT_DIR}/${DAEMON_NAME}" \
  ./cmd/sage-gui
test -f "${OUTPUT_DIR}/${DAEMON_NAME}"
test "$(find "${OUTPUT_DIR}" -mindepth 1 -maxdepth 1 -type f | wc -l | tr -d ' ')" = 1
