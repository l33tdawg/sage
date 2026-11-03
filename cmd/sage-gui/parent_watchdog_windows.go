//go:build windows

package main

// startParentWatchdog is a no-op on Windows. The tray that sets
// SAGE_PARENT_PID is macOS-only; the Windows launcher already owns child
// lifecycle through its supervisor.
func startParentWatchdog() {}
