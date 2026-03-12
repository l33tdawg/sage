#!/bin/bash
set -euo pipefail

# SAGE Linux Installer
# Installs sage-gui, sage-launcher, desktop entry, and icon.
#
# Usage:
#   ./install.sh              Install to ~/.local (user-local)
#   ./install.sh --system     Install to /usr/local (system-wide, needs sudo)
#   ./install.sh --uninstall  Remove user-local installation
#   ./install.sh --system --uninstall  Remove system-wide installation

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Defaults
SYSTEM_INSTALL=false
UNINSTALL=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --system)    SYSTEM_INSTALL=true ;;
        --uninstall) UNINSTALL=true ;;
        --help|-h)
            echo "Usage: $0 [--system] [--uninstall]"
            echo ""
            echo "  --system     Install system-wide to /usr/local/bin and /usr/share"
            echo "  --uninstall  Remove installed files"
            echo ""
            exit 0
            ;;
        *) echo "Unknown option: $arg"; exit 1 ;;
    esac
done

# Set paths based on install type
if [ "$SYSTEM_INSTALL" = true ]; then
    BIN_DIR="/usr/local/bin"
    APP_DIR="/usr/share/applications"
    ICON_DIR="/usr/share/icons/hicolor/scalable/apps"
else
    BIN_DIR="$HOME/.local/bin"
    APP_DIR="$HOME/.local/share/applications"
    ICON_DIR="$HOME/.local/share/icons/hicolor/scalable/apps"
fi

# --- Uninstall ---
if [ "$UNINSTALL" = true ]; then
    echo "==> Uninstalling SAGE..."

    for f in "$BIN_DIR/sage-gui" "$BIN_DIR/sage-launcher"; do
        if [ -f "$f" ]; then
            rm -f "$f"
            echo "    Removed $f"
        fi
    done

    if [ -f "$APP_DIR/sage.desktop" ]; then
        rm -f "$APP_DIR/sage.desktop"
        echo "    Removed $APP_DIR/sage.desktop"
    fi

    if [ -f "$ICON_DIR/sage.svg" ]; then
        rm -f "$ICON_DIR/sage.svg"
        echo "    Removed $ICON_DIR/sage.svg"
    fi

    # Update desktop database if available
    if command -v update-desktop-database &>/dev/null; then
        update-desktop-database "$APP_DIR" 2>/dev/null || true
    fi

    echo ""
    echo "==> SAGE uninstalled."
    echo "    User data in ~/.sage/ was NOT removed."
    echo "    To remove all data: rm -rf ~/.sage"
    exit 0
fi

# --- Install ---
echo "==> Installing SAGE..."
echo ""

# Check that binaries exist in the same directory as install.sh
if [ ! -f "$SCRIPT_DIR/sage-gui" ]; then
    echo "ERROR: sage-gui binary not found in $SCRIPT_DIR"
    echo "Make sure you extracted the full tarball before running install.sh"
    exit 1
fi

if [ ! -f "$SCRIPT_DIR/sage-launcher" ]; then
    echo "ERROR: sage-launcher binary not found in $SCRIPT_DIR"
    echo "Make sure you extracted the full tarball before running install.sh"
    exit 1
fi

# Create directories
mkdir -p "$BIN_DIR"
mkdir -p "$APP_DIR"
mkdir -p "$ICON_DIR"

# Copy binaries
echo "  [1/4] Installing binaries to $BIN_DIR..."
cp -f "$SCRIPT_DIR/sage-gui" "$BIN_DIR/sage-gui"
chmod +x "$BIN_DIR/sage-gui"
cp -f "$SCRIPT_DIR/sage-launcher" "$BIN_DIR/sage-launcher"
chmod +x "$BIN_DIR/sage-launcher"

# Copy desktop file
echo "  [2/4] Installing desktop entry to $APP_DIR..."
cp -f "$SCRIPT_DIR/sage.desktop" "$APP_DIR/sage.desktop"

# Copy icon
echo "  [3/4] Installing icon to $ICON_DIR..."
if [ -f "$SCRIPT_DIR/icon.svg" ]; then
    cp -f "$SCRIPT_DIR/icon.svg" "$ICON_DIR/sage.svg"
else
    echo "    WARNING: icon.svg not found — skipping icon installation"
fi

# Update desktop database
echo "  [4/4] Updating desktop database..."
if command -v update-desktop-database &>/dev/null; then
    update-desktop-database "$APP_DIR" 2>/dev/null || true
    echo "    Desktop database updated."
else
    echo "    update-desktop-database not found — skipping (icon may not appear until next login)"
fi

# Update icon cache if available
if command -v gtk-update-icon-cache &>/dev/null; then
    gtk-update-icon-cache -f -t "$(dirname "$(dirname "$ICON_DIR")")" 2>/dev/null || true
fi

echo ""
echo "==> SAGE installed successfully!"
echo ""

# Check if BIN_DIR is in PATH
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$BIN_DIR"; then
    echo "NOTE: $BIN_DIR is not in your PATH."
    echo "Add it by appending this to your ~/.bashrc or ~/.zshrc:"
    echo ""
    echo "    export PATH=\"$BIN_DIR:\$PATH\""
    echo ""
fi

echo "To start SAGE:"
echo "    sage-gui serve"
echo ""
echo "Or launch from your application menu."
