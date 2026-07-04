//go:build windows

package web

import "errors"

// errRestartUnsupported is returned by restartSelf on Windows. Windows has no
// exec(2), and auto-respawning the SAGE GUI process is unreliable (port re-bind
// race, tray/window handle continuity), so an in-process restart is not
// attempted here. Callers report a manual-restart message instead - the config
// change is already persisted, so a manual relaunch applies it.
var errRestartUnsupported = errors.New("in-process restart is not supported on windows")

// restartInProcessSupported reports whether SAGE can restart itself in place.
// Always false on Windows; see errRestartUnsupported.
func restartInProcessSupported() bool { return false }

// restartSelf is a no-op on Windows: it reports errRestartUnsupported so callers
// don't silently pretend a restart happened.
func restartSelf(_ string) error { return errRestartUnsupported }
