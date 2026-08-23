#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACKAGE_DIR="$ROOT_DIR/desktop/SAGECerebrumNative"
VERSION="${SAGE_NATIVE_VERSION:-12.0.0-beta.1}"
MARKETING_VERSION="${SAGE_NATIVE_MARKETING_VERSION:-12.0.0}"
CONFIGURATION="${SAGE_NATIVE_CONFIGURATION:-release}"
OUTPUT_DIR="${SAGE_NATIVE_OUTPUT_DIR:-$ROOT_DIR/dist/v12-native/$VERSION}"
APP_PATH="$OUTPUT_DIR/SAGE CEREBRUM Native.app"
CONTENTS="$APP_PATH/Contents"
EXECUTABLE="SAGECerebrumNative"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+-beta\.[0-9]+$ ]]; then
  echo "SAGE_NATIVE_VERSION must be numeric SemVer with a beta.N suffix, got: $VERSION" >&2
  exit 2
fi
if [[ "$MARKETING_VERSION" != "${VERSION%%-*}" ]]; then
  echo "SAGE_NATIVE_MARKETING_VERSION must match the numeric prefix of SAGE_NATIVE_VERSION" >&2
  exit 2
fi
BETA_NUMBER="${VERSION##*.}"
PATCH_NUMBER="${MARKETING_VERSION##*.}"
APPLE_BUILD_COMPONENT=$((10#$PATCH_NUMBER * 10000 + 10#$BETA_NUMBER))
APPLE_BUILD_VERSION="${MARKETING_VERSION%.*}.$APPLE_BUILD_COMPONENT"

export SWIFTPM_HOME="${SWIFTPM_HOME:-$PACKAGE_DIR/.swiftpm-home}"
export CLANG_MODULE_CACHE_PATH="${CLANG_MODULE_CACHE_PATH:-$PACKAGE_DIR/.clang-module-cache}"

swift build \
  --package-path "$PACKAGE_DIR" \
  --configuration "$CONFIGURATION" \
  --product "$EXECUTABLE" \
  --disable-sandbox

mkdir -p "$OUTPUT_DIR"
if [[ -e "$APP_PATH" ]]; then
  rm -rf "$APP_PATH"
fi
mkdir -p "$CONTENTS/MacOS" "$CONTENTS/Resources"

cp "$PACKAGE_DIR/.build/$CONFIGURATION/$EXECUTABLE" "$CONTENTS/MacOS/$EXECUTABLE"
cp "$ROOT_DIR/installer/macos/AppIcon.icns" "$CONTENTS/Resources/AppIcon.icns"
RESOURCE_BUNDLE="$PACKAGE_DIR/.build/$CONFIGURATION/SAGECerebrumNative_SAGECerebrumNative.bundle"
test -d "$RESOURCE_BUNDLE"
cp -R "$RESOURCE_BUNDLE" "$CONTENTS/Resources/"
PACKAGED_BRAIN="$CONTENTS/Resources/SAGECerebrumNative_SAGECerebrumNative.bundle/brain.obj"
test -r "$PACKAGED_BRAIN"
test -s "$PACKAGED_BRAIN"

PLIST="$CONTENTS/Info.plist"
plutil -create xml1 "$PLIST"
plutil -insert CFBundleDevelopmentRegion -string en "$PLIST"
plutil -insert CFBundleDisplayName -string "SAGE CEREBRUM Native" "$PLIST"
plutil -insert CFBundleExecutable -string "$EXECUTABLE" "$PLIST"
plutil -insert CFBundleIconFile -string AppIcon "$PLIST"
plutil -insert CFBundleIdentifier -string com.sage.cerebrum.beta "$PLIST"
plutil -insert CFBundleInfoDictionaryVersion -string 6.0 "$PLIST"
plutil -insert CFBundleName -string "SAGE CEREBRUM Native" "$PLIST"
plutil -insert CFBundlePackageType -string APPL "$PLIST"
plutil -insert CFBundleShortVersionString -string "$MARKETING_VERSION" "$PLIST"
plutil -insert CFBundleVersion -string "$APPLE_BUILD_VERSION" "$PLIST"
plutil -insert SAGEBetaVersion -string "$VERSION" "$PLIST"
plutil -insert LSMinimumSystemVersion -string 14.0 "$PLIST"
plutil -insert NSHighResolutionCapable -bool true "$PLIST"
plutil -insert NSPrincipalClass -string NSApplication "$PLIST"
plutil -insert NSSupportsAutomaticGraphicsSwitching -bool true "$PLIST"

plutil -lint "$PLIST"
test -x "$CONTENTS/MacOS/$EXECUTABLE"

echo "$APP_PATH"
