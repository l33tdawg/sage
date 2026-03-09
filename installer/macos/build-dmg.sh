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
# SAGE Launcher — copies binary out of .app bundle so the bundle can be replaced freely.
# The running sage-lite process lives in ~/.sage/bin/, NOT inside SAGE.app.
APP_BIN="$(dirname "$0")/sage-lite"
SAGE_DIR="$HOME/.sage"
SAGE_BIN="$SAGE_DIR/bin/sage-lite"
LOG_DIR="$SAGE_DIR/logs"
mkdir -p "$LOG_DIR" "$SAGE_DIR/bin"
LOG_FILE="$LOG_DIR/sage.log"
DASHBOARD_URL="http://localhost:8080/ui/"
PID_FILE="$SAGE_DIR/sage.pid"

open_dashboard() {
    for i in $(seq 1 30); do
        if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null | grep -q "200"; then
            open "$DASHBOARD_URL"
            return
        fi
        sleep 1
    done
    osascript -e 'display dialog "SAGE could not start. Check ~/.sage/logs/sage.log for details." with title "SAGE" with icon caution buttons {"OK"} default button "OK"'
}

stop_existing() {
    if [ -f "$PID_FILE" ]; then
        OLD_PID=$(cat "$PID_FILE")
        if kill -0 "$OLD_PID" 2>/dev/null; then
            kill "$OLD_PID" 2>/dev/null
            for i in $(seq 1 10); do
                kill -0 "$OLD_PID" 2>/dev/null || break
                sleep 0.5
            done
            kill -0 "$OLD_PID" 2>/dev/null && kill -9 "$OLD_PID" 2>/dev/null
        fi
        rm -f "$PID_FILE"
    fi
    ORPHAN_PID=$(lsof -ti tcp:8080 -s tcp:listen 2>/dev/null)
    if [ -n "$ORPHAN_PID" ]; then
        kill "$ORPHAN_PID" 2>/dev/null
        sleep 1
        kill -0 "$ORPHAN_PID" 2>/dev/null && kill -9 "$ORPHAN_PID" 2>/dev/null
    fi
}

# Handle "stop" argument
if [ "${1:-}" = "stop" ]; then
    stop_existing
    echo "SAGE stopped."
    exit 0
fi

# Copy binary from .app bundle to ~/.sage/bin/ (always update on launch)
cp -f "$APP_BIN" "$SAGE_BIN"
chmod +x "$SAGE_BIN"

# If SAGE is already running, check if it's the same version
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    # Compare binary checksums to detect version change
    RUNNING_HASH=$(md5 -q "$SAGE_BIN" 2>/dev/null)
    APP_HASH=$(md5 -q "$APP_BIN" 2>/dev/null)
    if [ "$RUNNING_HASH" = "$APP_HASH" ]; then
        open "$DASHBOARD_URL"
        exit 0
    fi
    # New version — stop old, start new
    echo "$(date): Upgrading SAGE — stopping old instance..." >> "$LOG_FILE"
    stop_existing
fi

# Check if port 8080 is in use by a non-SAGE process
if curl -s -o /dev/null http://localhost:8080/health 2>/dev/null; then
    PORT_PID=$(lsof -ti tcp:8080 -s tcp:listen 2>/dev/null)
    if [ -n "$PORT_PID" ]; then
        PORT_CMD=$(ps -p "$PORT_PID" -o command= 2>/dev/null)
        if echo "$PORT_CMD" | grep -q "sage-lite"; then
            # Existing sage-lite (maybe from CLI) — stop and replace with new version
            stop_existing
        fi
    fi
fi

# First run — need setup
if [ ! -f "$SAGE_DIR/config.yaml" ]; then
    "$SAGE_BIN" setup >> "$LOG_FILE" 2>&1
fi

# Start SAGE from ~/.sage/bin/ (NOT from inside .app bundle)
"$SAGE_BIN" serve >> "$LOG_FILE" 2>&1 &
SAGE_PID=$!
echo "$SAGE_PID" > "$PID_FILE"

(wait "$SAGE_PID" 2>/dev/null; rm -f "$PID_FILE") &

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

# Create the main installer script — handles stop, copy, and launch automatically
cat > "$DMG_TEMP/Install SAGE.command" << 'INSTALL'
#!/bin/bash
# SAGE Installer — stops any running instance, installs, and launches.
clear
echo ""
echo "  ╔═══════════════════════════════════════╗"
echo "  ║       SAGE Installer                  ║"
echo "  ╚═══════════════════════════════════════╝"
echo ""

PID_FILE="$HOME/.sage/sage.pid"
DMG_APP="$(dirname "$0")/SAGE.app"
DEST="/Applications/SAGE.app"

# --- Step 1: Stop any running SAGE process ---
echo "  [1/3] Checking for running SAGE..."

STOPPED=0

# Stop via PID file
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "        Stopping SAGE (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null
        for i in $(seq 1 10); do
            kill -0 "$OLD_PID" 2>/dev/null || break
            sleep 0.5
        done
        kill -0 "$OLD_PID" 2>/dev/null && kill -9 "$OLD_PID" 2>/dev/null
        STOPPED=1
    fi
    rm -f "$PID_FILE"
fi

# Kill any sage-lite on port 8080
ORPHAN_PID=$(lsof -ti tcp:8080 -s tcp:listen 2>/dev/null)
if [ -n "$ORPHAN_PID" ]; then
    ORPHAN_CMD=$(ps -p "$ORPHAN_PID" -o command= 2>/dev/null)
    if echo "$ORPHAN_CMD" | grep -q "sage-lite"; then
        echo "        Stopping sage-lite on port 8080 (PID $ORPHAN_PID)..."
        kill "$ORPHAN_PID" 2>/dev/null
        sleep 1
        kill -0 "$ORPHAN_PID" 2>/dev/null && kill -9 "$ORPHAN_PID" 2>/dev/null
        STOPPED=1
    fi
fi

# Also kill any other sage-lite processes
killall sage-lite 2>/dev/null && STOPPED=1

if [ "$STOPPED" -eq 1 ]; then
    echo "        SAGE stopped."
    sleep 1  # Brief pause to let macOS release file locks
else
    echo "        No running SAGE found."
fi

# --- Step 2: Copy SAGE.app to /Applications ---
echo "  [2/3] Installing SAGE.app to /Applications..."

if [ ! -d "$DMG_APP" ]; then
    echo ""
    echo "  ERROR: Cannot find SAGE.app in this disk image."
    echo "  Please re-download the DMG and try again."
    echo ""
    read -p "  Press Enter to close..."
    exit 1
fi

# Remove old version and copy new
rm -rf "$DEST"
cp -R "$DMG_APP" "$DEST"

if [ ! -d "$DEST" ]; then
    echo ""
    echo "  ERROR: Failed to copy SAGE.app to /Applications."
    echo "  You may need to run this with administrator privileges."
    echo ""
    read -p "  Press Enter to close..."
    exit 1
fi

echo "        Installed successfully."

# --- Step 3: Launch SAGE ---
echo "  [3/3] Launching SAGE..."
open "$DEST"

echo ""
echo "  ✓ SAGE has been installed and launched."
echo "    The CEREBRUM dashboard will open in your browser shortly."
echo ""
echo "    You can safely eject the disk image now."
echo ""
read -p "  Press Enter to close..."
INSTALL
chmod +x "$DMG_TEMP/Install SAGE.command"

# Create a README in the DMG
cat > "$DMG_TEMP/README.txt" << README
SAGE — Give Your AI a Persistent, Secure Memory
=================================================

INSTALL / UPDATE:
  Double-click "Install SAGE.command" — it handles everything:
  stops any running SAGE, installs the app, and launches it.

  Alternatively, you can drag SAGE.app to Applications manually.
  If you get "SAGE is still open", run "Install SAGE.command" instead.

On first launch, SAGE runs the setup wizard to configure your
personal memory node.

After setup, SAGE starts automatically and opens the CEREBRUM
Dashboard in your browser at http://localhost:8080.

You can also update from the dashboard: Settings > Update tab.

For Claude Code / CLI usage:
  ~/.sage/bin/sage-lite serve
  ~/.sage/bin/sage-lite mcp

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
