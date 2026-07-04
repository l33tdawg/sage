//go:build !windows

package web

import (
	"os"
	"syscall"
)

// restartInProcessSupported reports whether SAGE can restart itself in place on
// this OS. On unix-likes syscall.Exec replaces the process image (same PID), so
// the "Finish & restart" flows work. On Windows there is no exec(2) and a bare
// respawn of the GUI process is unreliable, so callers tell the operator to
// relaunch manually instead of pretending to restart.
func restartInProcessSupported() bool { return true }

// restartSelf re-execs the current binary with the same args and environment.
// It only returns on FAILURE - on success the process image is replaced.
func restartSelf(execPath string) error {
	return syscall.Exec(execPath, os.Args, os.Environ()) //nolint:gosec // execPath is the verified current binary
}
