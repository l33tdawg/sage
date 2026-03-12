#!/bin/bash
set -euo pipefail

# Build a Linux tarball release for SAGE.
#
# Cross-compiles sage-gui and sage-launcher for linux/amd64 and packages
# them with the desktop entry, install script, icon, and docs.
#
# Environment variables:
#   SAGE_VERSION  - Version string (e.g. "3.8.0")

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERSION="${SAGE_VERSION:-dev}"

BUILD_DIR="${PROJECT_ROOT}/dist/linux-amd64"
STAGING_DIR="${BUILD_DIR}/sage-${VERSION}-linux-amd64"

echo "==> Building SAGE ${VERSION} for Linux amd64"

# Clean previous build
rm -rf "$BUILD_DIR"
mkdir -p "$STAGING_DIR"

# Build flags
LDFLAGS="-s -w -X main.version=${VERSION} -X main.commit=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Cross-compile sage-gui
echo "==> Cross-compiling sage-gui for linux/amd64..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags "$LDFLAGS" \
    -o "${STAGING_DIR}/sage-gui" \
    "${PROJECT_ROOT}/cmd/sage-gui"

# Cross-compile sage-launcher (wrapper that starts sage-gui and opens browser)
echo "==> Cross-compiling sage-launcher for linux/amd64..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags "$LDFLAGS" \
    -o "${STAGING_DIR}/sage-launcher" \
    "${PROJECT_ROOT}/cmd/sage-launcher"

# Copy desktop integration files
echo "==> Packaging release files..."
cp "$SCRIPT_DIR/sage.desktop"  "$STAGING_DIR/"
cp "$SCRIPT_DIR/install.sh"    "$STAGING_DIR/"
chmod +x "$STAGING_DIR/install.sh"

# Copy icon from shared installer directory
if [ -f "${PROJECT_ROOT}/installer/icon.svg" ]; then
    cp "${PROJECT_ROOT}/installer/icon.svg" "$STAGING_DIR/"
else
    echo "    WARNING: installer/icon.svg not found"
fi

# Copy docs
if [ -f "${PROJECT_ROOT}/README.md" ]; then
    cp "${PROJECT_ROOT}/README.md" "$STAGING_DIR/"
fi
if [ -f "${PROJECT_ROOT}/LICENSE" ]; then
    cp "${PROJECT_ROOT}/LICENSE" "$STAGING_DIR/"
fi

# Create tarball
TARBALL="sage-gui_${VERSION}_linux_amd64.tar.gz"
echo "==> Creating tarball..."
tar -czf "${BUILD_DIR}/${TARBALL}" -C "$BUILD_DIR" "sage-${VERSION}-linux-amd64"

echo ""
echo "==> Done! Tarball created at:"
echo "    ${BUILD_DIR}/${TARBALL}"
echo ""
ls -lh "${BUILD_DIR}/${TARBALL}"
