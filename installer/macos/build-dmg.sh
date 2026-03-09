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
VERSION="${SAGE_VERSION:-dev}"
ARCH="${SAGE_ARCH:-$(uname -m)}"

# Normalize arch names
case "$ARCH" in
    amd64|x86_64) GOARCH="amd64"; ARCH_LABEL="x86_64" ;;
    arm64|aarch64) GOARCH="arm64"; ARCH_LABEL="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

APP_NAME="SAGE"
DMG_NAME="SAGE-${VERSION}-macOS-${ARCH_LABEL}"
BUILD_DIR="${PROJECT_ROOT}/dist/macos-${ARCH_LABEL}"
APP_DIR="${BUILD_DIR}/${APP_NAME}.app"

echo "==> Building SAGE ${VERSION} for macOS ${ARCH_LABEL}"

# Clean previous build
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Build the binary
echo "==> Compiling sage-lite..."
LDFLAGS="-s -w -X main.version=${VERSION} -X main.commit=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
CGO_ENABLED=0 GOOS=darwin GOARCH="$GOARCH" go build \
    -ldflags "$LDFLAGS" \
    -o "${BUILD_DIR}/sage-lite" \
    "${PROJECT_ROOT}/cmd/sage-lite"

# Create .app bundle structure
echo "==> Creating app bundle..."
mkdir -p "${APP_DIR}/Contents/MacOS"
mkdir -p "${APP_DIR}/Contents/Resources"

# Copy binary
cp "${BUILD_DIR}/sage-lite" "${APP_DIR}/Contents/MacOS/sage-lite"

# Create launcher script that opens Terminal with sage-lite
cat > "${APP_DIR}/Contents/MacOS/SAGE" << 'LAUNCHER'
#!/bin/bash
# SAGE Launcher — runs completely in the background, no Terminal.
SAGE_BIN="$(dirname "$0")/sage-lite"
LOG_DIR="$HOME/.sage/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/sage.log"
DASHBOARD_URL="http://localhost:8080/ui/"
PID_FILE="$HOME/.sage/sage.pid"

open_dashboard() {
    # Wait for the server to be ready, then open browser
    for i in $(seq 1 30); do
        if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null | grep -q "200"; then
            open "$DASHBOARD_URL"
            return
        fi
        sleep 1
    done
    # If server didn't start, show an error dialog
    osascript -e 'display dialog "SAGE could not start. Check ~/.sage/logs/sage.log for details." with title "SAGE" with icon caution buttons {"OK"} default button "OK"'
}

stop_existing() {
    # Stop any running sage-lite process (needed for updates)
    if [ -f "$PID_FILE" ]; then
        OLD_PID=$(cat "$PID_FILE")
        if kill -0 "$OLD_PID" 2>/dev/null; then
            kill "$OLD_PID" 2>/dev/null
            # Wait up to 5 seconds for graceful shutdown
            for i in $(seq 1 10); do
                kill -0 "$OLD_PID" 2>/dev/null || break
                sleep 0.5
            done
            # Force kill if still alive
            kill -0 "$OLD_PID" 2>/dev/null && kill -9 "$OLD_PID" 2>/dev/null
        fi
        rm -f "$PID_FILE"
    fi
    # Also check for any orphaned sage-lite processes on port 8080
    ORPHAN_PID=$(lsof -ti tcp:8080 -s tcp:listen 2>/dev/null)
    if [ -n "$ORPHAN_PID" ]; then
        kill "$ORPHAN_PID" 2>/dev/null
        sleep 1
        kill -0 "$ORPHAN_PID" 2>/dev/null && kill -9 "$ORPHAN_PID" 2>/dev/null
    fi
}

# Handle "stop" argument (used by update scripts or user: SAGE.app/Contents/MacOS/SAGE stop)
if [ "${1:-}" = "stop" ]; then
    stop_existing
    echo "SAGE stopped."
    exit 0
fi

# If SAGE is already running from THIS binary, just open the dashboard
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    # Check if the running process is the same binary (not an old version)
    RUNNING_BIN=$(ps -p "$(cat "$PID_FILE")" -o command= 2>/dev/null | awk '{print $1}')
    if [ "$RUNNING_BIN" = "$SAGE_BIN" ]; then
        open "$DASHBOARD_URL"
        exit 0
    fi
    # Different binary (old version) — stop it and start fresh
    echo "$(date): Stopping old SAGE instance for update..." >> "$LOG_FILE"
    stop_existing
fi

# Check if port 8080 is in use by a non-SAGE process
if curl -s -o /dev/null http://localhost:8080/health 2>/dev/null; then
    # If it's sage-lite from CLI, just open dashboard
    PORT_PID=$(lsof -ti tcp:8080 -s tcp:listen 2>/dev/null)
    if [ -n "$PORT_PID" ]; then
        PORT_CMD=$(ps -p "$PORT_PID" -o command= 2>/dev/null)
        if echo "$PORT_CMD" | grep -q "sage-lite"; then
            open "$DASHBOARD_URL"
            exit 0
        fi
    fi
fi

# First run — need setup
if [ ! -f "$HOME/.sage/config.yaml" ]; then
    # Run setup wizard (it opens its own browser window)
    "$SAGE_BIN" setup >> "$LOG_FILE" 2>&1
fi

# Start SAGE in the background
"$SAGE_BIN" serve >> "$LOG_FILE" 2>&1 &
SAGE_PID=$!
echo "$SAGE_PID" > "$PID_FILE"

# Clean up PID file when sage-lite exits
(wait "$SAGE_PID" 2>/dev/null; rm -f "$PID_FILE") &

# Open the dashboard once it's ready
open_dashboard &

exit 0
LAUNCHER
chmod +x "${APP_DIR}/Contents/MacOS/SAGE"

# Create Info.plist
cat > "${APP_DIR}/Contents/Info.plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>SAGE</string>
    <key>CFBundleDisplayName</key>
    <string>SAGE — AI Memory</string>
    <key>CFBundleIdentifier</key>
    <string>com.sage.personal</string>
    <key>CFBundleVersion</key>
    <string>${VERSION}</string>
    <key>CFBundleShortVersionString</key>
    <string>${VERSION}</string>
    <key>CFBundleExecutable</key>
    <string>SAGE</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon</string>
    <key>LSMinimumSystemVersion</key>
    <string>12.0</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>LSUIElement</key>
    <true/>
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
    codesign --force --options runtime --deep \
        --sign "$SIGN_IDENTITY" \
        --timestamp \
        "${APP_DIR}/Contents/MacOS/sage-lite"
    codesign --force --options runtime --deep \
        --sign "$SIGN_IDENTITY" \
        --timestamp \
        "${APP_DIR}"
    echo "    Verifying signature..."
    codesign --verify --deep --strict --verbose=2 "${APP_DIR}"
else
    echo "    (Skipping code signing — set SIGN_IDENTITY to enable)"
fi

# Create DMG
echo "==> Creating DMG..."
DMG_TEMP="${BUILD_DIR}/dmg-staging"
mkdir -p "$DMG_TEMP"
cp -R "${APP_DIR}" "$DMG_TEMP/"
ln -s /Applications "$DMG_TEMP/Applications"

# Create upgrade helper script
cat > "$DMG_TEMP/Upgrade SAGE.command" << 'UPGRADE'
#!/bin/bash
# SAGE Upgrade Helper — stops the running instance so you can replace it.
echo ""
echo "  SAGE Upgrade Helper"
echo "  ==================="
echo ""

PID_FILE="$HOME/.sage/sage.pid"

# Stop via PID file
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "  Stopping SAGE (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null
        sleep 2
        kill -0 "$OLD_PID" 2>/dev/null && kill -9 "$OLD_PID" 2>/dev/null
    fi
    rm -f "$PID_FILE"
fi

# Also kill any orphaned sage-lite
killall sage-lite 2>/dev/null

echo "  SAGE stopped."
echo ""
echo "  Now drag SAGE.app to Applications to replace the old version."
echo "  Then double-click SAGE in Applications to start."
echo ""
read -p "  Press Enter to close..."
UPGRADE
chmod +x "$DMG_TEMP/Upgrade SAGE.command"

# Create a README in the DMG
cat > "$DMG_TEMP/README.txt" << README
SAGE — Give Your AI a Persistent, Secure Memory
=================================================

INSTALL: Drag SAGE.app to Applications, then double-click to start.

On first launch, SAGE runs the setup wizard to configure your
personal memory node.

After setup, SAGE starts automatically and opens the Brain
Dashboard in your browser at http://localhost:8080.

UPDATE: If upgrading from a previous version:
  1. Open Terminal and run:
     /Applications/SAGE.app/Contents/MacOS/SAGE stop
  2. Drag the new SAGE.app to Applications (replace old)
  3. Double-click to start the new version.

  Or simply: killall sage-lite
  Then drag the new SAGE.app over the old one.

For Claude Code / CLI usage:
  /Applications/SAGE.app/Contents/MacOS/sage-lite serve
  /Applications/SAGE.app/Contents/MacOS/sage-lite mcp

More info: https://github.com/l33tdawg/sage
License: Apache 2.0
Author: Dhillon Andrew Kannabhiran
README

hdiutil create -volname "SAGE ${VERSION}" \
    -srcfolder "$DMG_TEMP" \
    -ov -format UDZO \
    "${BUILD_DIR}/${DMG_NAME}.dmg"

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
else
    echo "    (Skipping notarization — set NOTARIZE=1 to enable)"
fi

echo ""
echo "==> Done! DMG created at:"
echo "    ${BUILD_DIR}/${DMG_NAME}.dmg"
echo ""
ls -lh "${BUILD_DIR}/${DMG_NAME}.dmg"
