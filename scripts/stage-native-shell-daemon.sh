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
  *windows*) DAEMON_NAME=sage-gui.exe ;;
  *) DAEMON_NAME=sage-gui ;;
esac

VERSION=${SAGE_DAEMON_VERSION:-$(git -C "${REPO_ROOT}" describe --tags --always)}
COMMIT=$(git -C "${REPO_ROOT}" rev-parse --short HEAD)
BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
OUTPUT_DIR=${REPO_ROOT}/desktop/sage-shell/binaries

mkdir -p "${OUTPUT_DIR}"
env GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/sage-native-shell-gocache}" go build \
  -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${BUILD_DATE}" \
  -o "${OUTPUT_DIR}/${DAEMON_NAME}" \
  ./cmd/sage-gui
