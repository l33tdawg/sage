#!/usr/bin/env bash
# Build and verify the private macOS v12 beta tester artifact.
#
# This lane is intentionally separate from the stable v* release workflow. It
# produces a beta-only app identity, stages the exact same-version daemon into
# the Tauri resource tree, and writes content-addressed local evidence.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
TARGET=${SAGE_NATIVE_BETA_TARGET:-aarch64-apple-darwin}
VERSION=${SAGE_NATIVE_BETA_VERSION:-12.0.0-beta.1}
PRODUCT_NAME=${SAGE_NATIVE_BETA_PRODUCT_NAME:-SAGE CEREBRUM Beta}
IDENTIFIER=${SAGE_NATIVE_BETA_IDENTIFIER:-com.sage.cerebrum.prototype.beta}
DIST_ROOT=${SAGE_NATIVE_BETA_DIST_ROOT:-${REPO_ROOT}/dist/v12-beta}

if [ "$(uname -s)" != "Darwin" ]; then
  echo "v12 macOS beta packaging must run on macOS" >&2
  exit 2
fi

case "${TARGET}" in
  aarch64-apple-darwin) ARCH_LABEL=aarch64 ;;
  x86_64-apple-darwin) ARCH_LABEL=x86_64 ;;
  *)
    echo "unsupported macOS beta target: ${TARGET}" >&2
    exit 2
    ;;
esac

if [[ ! "${VERSION}" =~ ^12\.0\.[0-9]+-beta\.[0-9]+$ ]]; then
  echo "SAGE_NATIVE_BETA_VERSION must be a 12.0.x-beta.N semver, got: ${VERSION}" >&2
  exit 2
fi

# Apple bundle versions are numeric even when the SAGE release and bundled
# daemon use a SemVer prerelease label. Preserve the full beta identity in the
# artifact name/evidence while mapping it to monotonically increasing plist
# values (for example beta.1 => CFBundleVersion 12.0.1).
MARKETING_VERSION=${VERSION%%-*}
PATCH_VERSION=${MARKETING_VERSION##*.}
BETA_NUMBER=${VERSION##*.}
APPLE_BUILD_COMPONENT=$((10#${PATCH_VERSION} * 10000 + 10#${BETA_NUMBER}))
APPLE_BUILD_VERSION=12.0.${APPLE_BUILD_COMPONENT}

CURRENT_BRANCH=$(git -C "${REPO_ROOT}" branch --show-current)
if [ "${CURRENT_BRANCH}" != "v12-beta" ] && [ "${SAGE_NATIVE_BETA_ALLOW_BRANCH:-0}" != "1" ]; then
  echo "refusing to build a v12 beta from branch ${CURRENT_BRANCH}; use v12-beta or set SAGE_NATIVE_BETA_ALLOW_BRANCH=1" >&2
  exit 2
fi

if [ "${IDENTIFIER}" = "com.sage.native-preview" ] || [ "${IDENTIFIER}" = "com.sage.brain" ]; then
  echo "v12 beta must not reuse a stable or preview application identifier" >&2
  exit 2
fi

BUNDLE_ROOT=${REPO_ROOT}/desktop/sage-shell/target/${TARGET}/release/bundle
EVIDENCE_ROOT=${DIST_ROOT}/${VERSION}/${TARGET}
mkdir -p "${EVIDENCE_ROOT}"

echo "==> Staging SAGE daemon ${VERSION} for ${TARGET}"
(
  cd "${REPO_ROOT}"
  SAGE_DAEMON_VERSION="${VERSION}" scripts/stage-native-shell-daemon.sh "${TARGET}"
)

echo "==> Building ${PRODUCT_NAME} ${VERSION}"
(
  cd "${REPO_ROOT}/desktop/sage-shell"
  cargo tauri build --ci \
    --no-sign \
    --target "${TARGET}" \
    --bundles app \
    --config "{\"version\":\"${MARKETING_VERSION}\",\"productName\":\"${PRODUCT_NAME}\",\"identifier\":\"${IDENTIFIER}\",\"bundle\":{\"macOS\":{\"bundleVersion\":\"${APPLE_BUILD_VERSION}\"}}}"
)

test -d "${BUNDLE_ROOT}/macos"
mkdir -p "${BUNDLE_ROOT}/dmg"

APP_PATH=$(find "${BUNDLE_ROOT}/macos" -maxdepth 1 -type d -name '*.app' -print -quit)
test -n "${APP_PATH}"
PLIST=${APP_PATH}/Contents/Info.plist
test "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleIdentifier' "${PLIST}")" = "${IDENTIFIER}"
test "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "${PLIST}")" = "${MARKETING_VERSION}"
test "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleVersion' "${PLIST}")" = "${APPLE_BUILD_VERSION}"
test "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleName' "${PLIST}")" = "${PRODUCT_NAME}"

DMG_PATH=${BUNDLE_ROOT}/dmg/SAGE-CEREBRUM-Beta_${VERSION}_${ARCH_LABEL}.dmg
rm -f -- "${BUNDLE_ROOT}/dmg"/*.dmg
DMG_STAGE=$(mktemp -d "${TMPDIR:-/tmp}/sage-v12-beta-dmg.XXXXXX")
cleanup_dmg_stage() {
  rm -rf -- "${DMG_STAGE}"
}
trap cleanup_dmg_stage EXIT INT TERM
/usr/bin/ditto "${APP_PATH}" "${DMG_STAGE}/$(basename "${APP_PATH}")"
ln -s /Applications "${DMG_STAGE}/Applications"
hdiutil create -size 1024m \
  -volname "SAGE CEREBRUM Beta ${VERSION}" \
  -srcfolder "${DMG_STAGE}" \
  -ov -format UDZO "${DMG_PATH}" >/dev/null

scripts/verify-native-shell-bundle.sh \
  "${TARGET}" "${BUNDLE_ROOT}" "${VERSION}" \
  "${EVIDENCE_ROOT}/native-shell-release-pair-app.json" app
scripts/verify-native-shell-bundle.sh \
  "${TARGET}" "${BUNDLE_ROOT}" "${VERSION}" \
  "${EVIDENCE_ROOT}/native-shell-release-pair-dmg.json" dmg

cp -p "${DMG_PATH}" "${EVIDENCE_ROOT}/"
(
  cd "${EVIDENCE_ROOT}"
  shasum -a 256 "$(basename "${DMG_PATH}")" > "$(basename "${DMG_PATH}").sha256"
)
printf '%s\n' "$(basename "${APP_PATH}")" > "${EVIDENCE_ROOT}/app-path.txt"
printf '%s\n' "$(basename "${DMG_PATH}")" > "${EVIDENCE_ROOT}/dmg-path.txt"

echo "v12 beta artifact verified"
echo "  app: ${APP_PATH}"
echo "  dmg: ${EVIDENCE_ROOT}/$(basename "${DMG_PATH}")"
echo "  evidence: ${EVIDENCE_ROOT}"
