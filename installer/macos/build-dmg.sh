#!/bin/bash
set -euo pipefail

# Build a signed macOS .dmg installer for SAGE.
#
# Prerequisites:
#   - Xcode command line tools
#   - Developer ID Application certificate in keychain
#   - Apple notarytool credentials (for notarization)
#
# Environment variables:
#   SAGE_VERSION      - Version string (e.g. "2.1.0")
#   SAGE_ARCH         - Target architecture: "amd64" or "arm64" (default: current)
#   SIGN_IDENTITY     - Code signing identity (e.g. "Developer ID Application: Your Name (TEAMID)")
#   NOTARIZE          - Set to "1" to notarize (requires APPLE_ID, APPLE_TEAM_ID, APPLE_PASSWORD)
#   APPLE_ID          - Apple ID email for notarization
#   APPLE_TEAM_ID     - Apple Developer Team ID
#   APPLE_PASSWORD    - App-specific password for notarization

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ASSET_VERSION="${SAGE_VERSION:-dev}"
VERSION="${ASSET_VERSION#v}"
ARCH="${SAGE_ARCH:-$(uname -m)}"

bundle_byte_manifest() {
    local bundle_root=$1
    (
        cd "$bundle_root"
        find . -type f -print | LC_ALL=C sort | while IFS= read -r rel; do
            hash=$(shasum -a 256 "$rel" | awk '{print $1}')
            mode=$(stat -f '%Lp' "$rel")
            printf 'F %s %s %s\n' "$mode" "$hash" "$rel"
        done
        find . -type l -print | LC_ALL=C sort | while IFS= read -r rel; do
            mode=$(stat -f '%Lp' "$rel")
            printf 'L %s %s -> %s\n' "$mode" "$rel" "$(readlink "$rel")"
        done
    )
}

verify_app_release_metadata() {
    local app_path=$1
    local expected_version=$2
    local version_output
    test "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleIdentifier' "$app_path/Contents/Info.plist")" = "com.sage.brain"
    test "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleExecutable' "$app_path/Contents/Info.plist")" = "sage-tray"
    test "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleVersion' "$app_path/Contents/Info.plist")" = "$expected_version"
    test "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$app_path/Contents/Info.plist")" = "$expected_version"
    version_output=$("$app_path/Contents/MacOS/sage-gui" version)
    test "$(printf '%s\n' "$version_output" | awk 'NR == 1 { print $1 }')" = "sage-gui"
    test "$(printf '%s\n' "$version_output" | awk 'NR == 1 { print $2 }')" = "$expected_version"
}

require_writable_apfs_path() {
    local path=$1
    local device
    test -d "$path" && test -w "$path"
    device=$(df "$path" | awk 'END { print $1 }')
    test -n "$device"
    /usr/sbin/diskutil info "$device" | grep -Eq 'File System Personality:[[:space:]]+APFS'
}

# Release builds set NOTARIZE=1. Keep unsigned local developer builds possible,
# but make the notarized path fail closed before compilation when any signing
# input is absent.
if [ "${NOTARIZE:-0}" = "1" ]; then
    : "${SIGN_IDENTITY:?NOTARIZE=1 requires SIGN_IDENTITY}"
    : "${APPLE_ID:?NOTARIZE=1 requires APPLE_ID}"
    : "${APPLE_TEAM_ID:?NOTARIZE=1 requires APPLE_TEAM_ID}"
    : "${APPLE_PASSWORD:?NOTARIZE=1 requires APPLE_PASSWORD}"
fi

# Normalize arch names
case "$ARCH" in
    amd64|x86_64) GOARCH="amd64"; ARCH_LABEL="x86_64" ;;
    arm64|aarch64) GOARCH="arm64"; ARCH_LABEL="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

APP_NAME="SAGE"
DMG_NAME="SAGE-${ASSET_VERSION}-macOS-${ARCH_LABEL}"
BUILD_DIR="${PROJECT_ROOT}/dist/macos-${ARCH_LABEL}"
APP_DIR="${BUILD_DIR}/${APP_NAME}.app"

echo "==> Building SAGE ${VERSION} for macOS ${ARCH_LABEL}"

# Clean previous build
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Build the binary
echo "==> Compiling sage-gui..."
LDFLAGS="-s -w -X main.version=${VERSION} -X main.commit=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
CGO_ENABLED=0 GOOS=darwin GOARCH="$GOARCH" go build \
    -ldflags "$LDFLAGS" \
    -o "${BUILD_DIR}/sage-gui" \
    "${PROJECT_ROOT}/cmd/sage-gui"

# Create .app bundle structure
echo "==> Creating app bundle..."
mkdir -p "${APP_DIR}/Contents/MacOS"
mkdir -p "${APP_DIR}/Contents/Resources"

# Copy sage-gui binary
cp "${BUILD_DIR}/sage-gui" "${APP_DIR}/Contents/MacOS/sage-gui"

# Compile native Swift dock app (sage-tray)
echo "==> Compiling native dock app (sage-tray)..."
SWIFT_SRC="${PROJECT_ROOT}/cmd/sage-tray/main.swift"
if [ -f "$SWIFT_SRC" ]; then
    if [ "$GOARCH" = "arm64" ]; then
        SWIFT_ARCH="arm64"
    else
        SWIFT_ARCH="x86_64"
    fi
    swiftc -O -target "${SWIFT_ARCH}-apple-macosx12.0" \
        -o "${APP_DIR}/Contents/MacOS/sage-tray" \
        "$SWIFT_SRC" -framework Cocoa
else
    echo "    WARNING: cmd/sage-tray/main.swift not found — falling back to launcher script"
fi

# Create Info.plist — native dock app (LSUIElement=false shows in dock)
cat > "${APP_DIR}/Contents/Info.plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>SAGE</string>
    <key>CFBundleDisplayName</key>
    <string>SAGE Brain</string>
    <key>CFBundleIdentifier</key>
    <string>com.sage.brain</string>
    <key>CFBundleVersion</key>
    <string>${VERSION}</string>
    <key>CFBundleShortVersionString</key>
    <string>${VERSION}</string>
    <key>CFBundleExecutable</key>
    <string>sage-tray</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon</string>
    <key>LSMinimumSystemVersion</key>
    <string>12.0</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>LSUIElement</key>
    <false/>
    <key>NSHumanReadableCopyright</key>
    <string>Copyright 2024-2026 Dhillon Andrew Kannabhiran. Apache 2.0 License.</string>
</dict>
</plist>
PLIST

# Copy icon if it exists
if [ -f "${SCRIPT_DIR}/AppIcon.icns" ]; then
    cp "${SCRIPT_DIR}/AppIcon.icns" "${APP_DIR}/Contents/Resources/AppIcon.icns"
else
    echo "    (No AppIcon.icns found — DMG will use default icon)"
fi

# Code sign if identity provided
if [ -n "${SIGN_IDENTITY:-}" ]; then
    echo "==> Code signing with: ${SIGN_IDENTITY}"
    # Sign leaves first and the bundle last. Signing-time --deep is deprecated
    # and can recursively rewrite nested signatures after the outer seal was
    # computed, producing a bundle that passes an immediate check but later
    # fails once macOS has re-read every CodeDirectory.
    codesign --force --options runtime \
        --sign "$SIGN_IDENTITY" \
        --timestamp \
        "${APP_DIR}/Contents/MacOS/sage-gui"
    if [ -f "${APP_DIR}/Contents/MacOS/sage-tray" ]; then
        codesign --force --options runtime \
            --sign "$SIGN_IDENTITY" \
            --timestamp \
            "${APP_DIR}/Contents/MacOS/sage-tray"
    fi
    codesign --force --options runtime \
        --sign "$SIGN_IDENTITY" \
        --timestamp \
        "${APP_DIR}"
    echo "    Verifying signature..."
    codesign --verify --deep --strict --verbose=2 "${APP_DIR}"
    app_identity=$(codesign -dv --verbose=4 "${APP_DIR}" 2>&1)
    printf '%s\n' "$app_identity" | grep -Fx "Identifier=com.sage.brain"
    if [ "${NOTARIZE:-0}" = "1" ]; then
        printf '%s\n' "$app_identity" | grep -Fx "TeamIdentifier=${APPLE_TEAM_ID}"
    fi
    for leaf_spec in \
        "${APP_DIR}/Contents/MacOS/sage-gui:sage-gui" \
        "${APP_DIR}/Contents/MacOS/sage-tray:com.sage.brain"; do
        leaf=${leaf_spec%:*}
        expected_identifier=${leaf_spec##*:}
        test -f "$leaf" && test ! -L "$leaf" && test -x "$leaf"
        test "$(stat -f '%Lp' "$leaf")" = "755"
        codesign --verify --strict --verbose=2 "$leaf"
        leaf_identity=$(codesign -dv --verbose=4 "$leaf" 2>&1)
        printf '%s\n' "$leaf_identity" | grep -Fx "Identifier=${expected_identifier}"
        if [ "${NOTARIZE:-0}" = "1" ]; then
            printf '%s\n' "$leaf_identity" | grep -Fx "TeamIdentifier=${APPLE_TEAM_ID}"
        fi
    done
    verify_app_release_metadata "${APP_DIR}" "${VERSION}"
else
    echo "    (Skipping code signing — set SIGN_IDENTITY to enable)"
fi

# Create DMG
echo "==> Creating DMG..."
DMG_TEMP="${BUILD_DIR}/dmg-staging"
mkdir -p "$DMG_TEMP"
cp -R "${APP_DIR}" "$DMG_TEMP/"
ln -s /Applications "$DMG_TEMP/Applications"

# Create a README in the DMG
cat > "$DMG_TEMP/README.txt" << README
SAGE — Give Your AI a Persistent, Secure Memory
=================================================

INSTALL / UPDATE:
  1. If SAGE is already running, right-click its dock icon and choose Quit.
  2. Drag SAGE.app to the Applications folder.
  3. Open SAGE from Applications (or Launchpad).

On first launch, SAGE runs the setup wizard to configure your
personal memory node.

After setup, SAGE starts automatically and opens the CEREBRUM
Dashboard in your browser at http://localhost:8080.

You can also check for updates from CEREBRUM: Settings > Update. On macOS,
that screen downloads the signed DMG; fully quit SAGE, drag SAGE.app to
Applications, then reopen it. CEREBRUM never replaces its own running app.

For Claude Code / CLI usage:
  ~/.sage/bin/sage-gui serve
  ~/.sage/bin/sage-gui mcp

More info: https://github.com/l33tdawg/sage
License: Apache 2.0
Author: Dhillon Andrew Kannabhiran
README

# hdiutil's inferred srcfolder capacity can be too tight for cross-compiled
# Intel app bundles, failing during the copy with ENOSPC even when the runner
# itself has ample free disk. The temporary 1 GiB filesystem is compressed to
# the actual payload size by UDZO.
hdiutil create -size 1024m -volname "SAGE ${VERSION}" \
    -srcfolder "$DMG_TEMP" \
    -ov -format UDZO \
    "${BUILD_DIR}/${DMG_NAME}.dmg"

# Verify the exact bundle serialized into the completed image, not only the
# pre-image source directory. This catches packaging-time signature drift
# before notarization or publication.
if [ -n "${SIGN_IDENTITY:-}" ]; then
    VERIFY_MOUNT="$(mktemp -d "${TMPDIR:-/tmp}/sage-dmg-verify.XXXXXX")"
    COPY_VERIFY_ROOT=
    cleanup_verify_mount() {
        if [ -n "${COPY_VERIFY_ROOT:-}" ]; then
            rm -rf "$COPY_VERIFY_ROOT"
            COPY_VERIFY_ROOT=
        fi
        hdiutil detach "$VERIFY_MOUNT" >/dev/null 2>&1 || true
        rmdir "$VERIFY_MOUNT" >/dev/null 2>&1 || true
    }
    trap cleanup_verify_mount EXIT
    hdiutil attach -readonly -nobrowse -mountpoint "$VERIFY_MOUNT" \
        "${BUILD_DIR}/${DMG_NAME}.dmg" >/dev/null
    codesign --verify --deep --strict --verbose=2 "$VERIFY_MOUNT/SAGE.app"
    app_identity=$(codesign -dv --verbose=4 "$VERIFY_MOUNT/SAGE.app" 2>&1)
    printf '%s\n' "$app_identity" | grep -Fx "Identifier=com.sage.brain"
    if [ "${NOTARIZE:-0}" = "1" ]; then
        printf '%s\n' "$app_identity" | grep -Fx "TeamIdentifier=${APPLE_TEAM_ID}"
    fi
    for leaf_spec in \
        "$VERIFY_MOUNT/SAGE.app/Contents/MacOS/sage-gui:sage-gui" \
        "$VERIFY_MOUNT/SAGE.app/Contents/MacOS/sage-tray:com.sage.brain"; do
        leaf=${leaf_spec%:*}
        expected_identifier=${leaf_spec##*:}
        test -f "$leaf" && test ! -L "$leaf" && test -x "$leaf"
        test "$(stat -f '%Lp' "$leaf")" = "755"
        codesign --verify --strict --verbose=2 "$leaf"
        leaf_identity=$(codesign -dv --verbose=4 "$leaf" 2>&1)
        printf '%s\n' "$leaf_identity" | grep -Fx "Identifier=${expected_identifier}"
        if [ "${NOTARIZE:-0}" = "1" ]; then
            printf '%s\n' "$leaf_identity" | grep -Fx "TeamIdentifier=${APPLE_TEAM_ID}"
        fi
    done
    verify_app_release_metadata "$VERIFY_MOUNT/SAGE.app" "${VERSION}"

    # Prove that the signed DMG survives an ordinary writable-volume install
    # copy on a fresh APFS inode. Read-only mount verification alone does not
    # catch vnode/path cache failures that appear only after ditto and first
    # execution.
    COPY_VERIFY_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/sage-dmg-copy-verify.XXXXXX")"
    require_writable_apfs_path "$COPY_VERIFY_ROOT"
    COPY_VERIFY_APP="${COPY_VERIFY_ROOT}/SAGE.app"
    /usr/bin/ditto "$VERIFY_MOUNT/SAGE.app" "$COPY_VERIFY_APP"
    bundle_byte_manifest "$VERIFY_MOUNT/SAGE.app" > "${COPY_VERIFY_ROOT}/mounted.manifest"
    bundle_byte_manifest "$COPY_VERIFY_APP" > "${COPY_VERIFY_ROOT}/copied.manifest"
    diff -u "${COPY_VERIFY_ROOT}/mounted.manifest" "${COPY_VERIFY_ROOT}/copied.manifest"
    codesign --verify --deep --strict --verbose=2 "$COPY_VERIFY_APP"
    app_identity=$(codesign -dv --verbose=4 "$COPY_VERIFY_APP" 2>&1)
    printf '%s\n' "$app_identity" | grep -Fx "Identifier=com.sage.brain"
    if [ "${NOTARIZE:-0}" = "1" ]; then
        printf '%s\n' "$app_identity" | grep -Fx "TeamIdentifier=${APPLE_TEAM_ID}"
    fi
    for leaf_spec in \
        "$COPY_VERIFY_APP/Contents/MacOS/sage-gui:sage-gui" \
        "$COPY_VERIFY_APP/Contents/MacOS/sage-tray:com.sage.brain"; do
        leaf=${leaf_spec%:*}
        expected_identifier=${leaf_spec##*:}
        test -f "$leaf" && test ! -L "$leaf" && test -x "$leaf"
        test "$(stat -f '%Lp' "$leaf")" = "755"
        codesign --verify --strict --verbose=2 "$leaf"
        leaf_identity=$(codesign -dv --verbose=4 "$leaf" 2>&1)
        printf '%s\n' "$leaf_identity" | grep -Fx "Identifier=${expected_identifier}"
        if [ "${NOTARIZE:-0}" = "1" ]; then
            printf '%s\n' "$leaf_identity" | grep -Fx "TeamIdentifier=${APPLE_TEAM_ID}"
        fi
    done
    verify_app_release_metadata "$COPY_VERIFY_APP" "${VERSION}"
    codesign --verify --deep --strict --verbose=2 "$COPY_VERIFY_APP"
    codesign --verify --strict --verbose=2 "$COPY_VERIFY_APP/Contents/MacOS/sage-gui"
    codesign --verify --strict --verbose=2 "$COPY_VERIFY_APP/Contents/MacOS/sage-tray"
    rm -rf "$COPY_VERIFY_ROOT"
    COPY_VERIFY_ROOT=
    hdiutil detach "$VERIFY_MOUNT" >/dev/null
    rmdir "$VERIFY_MOUNT"
    trap - EXIT
fi

# Notarize if requested
if [ "${NOTARIZE:-}" = "1" ] && [ -n "${APPLE_ID:-}" ]; then
    echo "==> Notarizing DMG..."
    xcrun notarytool submit "${BUILD_DIR}/${DMG_NAME}.dmg" \
        --apple-id "$APPLE_ID" \
        --team-id "$APPLE_TEAM_ID" \
        --password "$APPLE_PASSWORD" \
        --wait

    echo "==> Stapling notarization ticket..."
    xcrun stapler staple "${BUILD_DIR}/${DMG_NAME}.dmg"
    xcrun stapler validate "${BUILD_DIR}/${DMG_NAME}.dmg"
else
    echo "    (Skipping notarization — set NOTARIZE=1 to enable)"
fi

echo ""
echo "==> Done! DMG created at:"
echo "    ${BUILD_DIR}/${DMG_NAME}.dmg"
echo ""
ls -lh "${BUILD_DIR}/${DMG_NAME}.dmg"
