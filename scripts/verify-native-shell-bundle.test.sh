#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
FIXTURE_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-bundle-test.XXXXXX")
cleanup() {
  rm -rf -- "${FIXTURE_ROOT}"
}
trap cleanup EXIT INT TERM

APP_PATH=${FIXTURE_ROOT}/bundle/macos/SAGE.app
DAEMON_PATH=${APP_PATH}/Contents/Resources/binaries/sage-gui
SHELL_PATH=${APP_PATH}/Contents/MacOS/SAGE
EVIDENCE_PATH=${FIXTURE_ROOT}/bundle/native-shell-pair.json
mkdir -p "$(dirname "${DAEMON_PATH}")"
mkdir -p "$(dirname "${SHELL_PATH}")"
printf '%s\n' '#!/usr/bin/env bash' 'echo "sage-gui 11.11.0-test.1 (commit fixture, built fixture)"' > "${DAEMON_PATH}"
printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "${SHELL_PATH}"
chmod +x "${DAEMON_PATH}" "${SHELL_PATH}"

"${REPO_ROOT}/scripts/verify-native-shell-bundle.sh" \
  aarch64-apple-darwin \
  "${FIXTURE_ROOT}/bundle" \
  11.11.0-test.1 \
  "${EVIDENCE_PATH}"

test -s "${EVIDENCE_PATH}"
grep -F '"schema": "dev.sage.native-shell.release-pair/v1"' "${EVIDENCE_PATH}" >/dev/null
grep -F '"build_version": "11.11.0-test.1"' "${EVIDENCE_PATH}" >/dev/null
grep -F '"kind": "app-executable"' "${EVIDENCE_PATH}" >/dev/null
grep -F '"path": "macos/SAGE.app/Contents/MacOS/SAGE"' "${EVIDENCE_PATH}" >/dev/null
grep -F '"package_path": "SAGE.app/Contents/Resources/binaries/sage-gui"' "${EVIDENCE_PATH}" >/dev/null
grep -E '"sha256": "[0-9a-f]{64}"' "${EVIDENCE_PATH}" >/dev/null

if "${REPO_ROOT}/scripts/verify-native-shell-bundle.sh" \
  aarch64-apple-darwin \
  "${FIXTURE_ROOT}/bundle" \
  11.11.0-wrong; then
  echo "bundle verifier accepted a mismatched daemon version" >&2
  exit 1
fi

APPIMAGE_PATH=${FIXTURE_ROOT}/bundle/appimage/SAGE_11.11.0-test.1_amd64.AppImage
APPIMAGE_EVIDENCE_PATH=${FIXTURE_ROOT}/bundle/native-shell-pair-appimage.json
mkdir -p "$(dirname "${APPIMAGE_PATH}")"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'test "${1:-}" = "--appimage-extract"' \
  'mkdir -p squashfs-root/usr/lib/SAGE/binaries' \
  'printf '\''%s\n'\'' '\''#!/usr/bin/env bash'\'' '\''echo "sage-gui 11.11.0-test.1 (commit fixture, built fixture)"'\'' > squashfs-root/usr/lib/SAGE/binaries/sage-gui' \
  'chmod +x squashfs-root/usr/lib/SAGE/binaries/sage-gui' > "${APPIMAGE_PATH}"
chmod +x "${APPIMAGE_PATH}"

"${REPO_ROOT}/scripts/verify-native-shell-bundle.sh" \
  x86_64-unknown-linux-gnu \
  "${FIXTURE_ROOT}/bundle" \
  11.11.0-test.1 \
  "${APPIMAGE_EVIDENCE_PATH}" \
  appimage

test -s "${APPIMAGE_EVIDENCE_PATH}"
grep -F '"kind": "appimage"' "${APPIMAGE_EVIDENCE_PATH}" >/dev/null
grep -F '"path": "appimage/SAGE_11.11.0-test.1_amd64.AppImage"' "${APPIMAGE_EVIDENCE_PATH}" >/dev/null
grep -F '"package_path": "usr/lib/SAGE/binaries/sage-gui"' "${APPIMAGE_EVIDENCE_PATH}" >/dev/null
