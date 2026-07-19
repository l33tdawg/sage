#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
FIXTURE_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-bundle-test.XXXXXX")
cleanup() {
  rm -rf -- "${FIXTURE_ROOT}"
}
trap cleanup EXIT INT TERM

DAEMON_PATH=${FIXTURE_ROOT}/bundle/macos/SAGE.app/Contents/Resources/binaries/sage-gui
mkdir -p "$(dirname "${DAEMON_PATH}")"
printf '%s\n' '#!/usr/bin/env bash' 'echo "sage-gui 11.11.0-test.1 (commit fixture, built fixture)"' > "${DAEMON_PATH}"
chmod +x "${DAEMON_PATH}"

"${REPO_ROOT}/scripts/verify-native-shell-bundle.sh" \
  aarch64-apple-darwin \
  "${FIXTURE_ROOT}/bundle" \
  11.11.0-test.1

if "${REPO_ROOT}/scripts/verify-native-shell-bundle.sh" \
  aarch64-apple-darwin \
  "${FIXTURE_ROOT}/bundle" \
  11.11.0-wrong; then
  echo "bundle verifier accepted a mismatched daemon version" >&2
  exit 1
fi
