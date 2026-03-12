#!/bin/bash
set -euo pipefail

# Build a Windows .exe installer for SAGE using NSIS.
#
# This can run on macOS/Linux with cross-compilation + NSIS installed:
#   brew install makensis   (macOS)
#   apt install nsis        (Ubuntu)
#
# Environment variables:
#   SAGE_VERSION  - Version string (e.g. "2.1.0")

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERSION="${SAGE_VERSION:-dev}"

BUILD_DIR="${PROJECT_ROOT}/dist/windows-amd64"

echo "==> Building SAGE ${VERSION} for Windows amd64"

# Clean previous build
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Build the binaries
echo "==> Cross-compiling sage-gui for Windows..."
LDFLAGS="-s -w -X main.version=${VERSION} -X main.commit=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build \
    -ldflags "$LDFLAGS" \
    -o "${BUILD_DIR}/sage-gui.exe" \
    "${PROJECT_ROOT}/cmd/sage-gui"

echo "==> Cross-compiling sage-cli for Windows..."
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build \
    -ldflags "$LDFLAGS" \
    -o "${BUILD_DIR}/sage-cli.exe" \
    "${PROJECT_ROOT}/cmd/sage-cli"

echo "==> Cross-compiling sage-launcher for Windows (GUI mode, no console)..."
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build \
    -ldflags "$LDFLAGS -H=windowsgui" \
    -o "${BUILD_DIR}/sage-launcher.exe" \
    "${PROJECT_ROOT}/cmd/sage-launcher"

# Copy NSIS script and icon to build dir
cp "$SCRIPT_DIR/sage-installer.nsi" "$BUILD_DIR/"
cp "$SCRIPT_DIR/sage.ico" "$BUILD_DIR/"

# Build installer with NSIS
echo "==> Building NSIS installer..."
cd "$BUILD_DIR"
makensis -DVERSION="${VERSION}" sage-installer.nsi

echo ""
echo "==> Done! Installer created at:"
echo "    ${BUILD_DIR}/SAGE-${VERSION}-Windows-Setup.exe"
echo ""
ls -lh "${BUILD_DIR}/SAGE-${VERSION}-Windows-Setup.exe"
