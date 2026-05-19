//go:build !windows

package main

import "syscall"

// defaultExecer is the production Execer. On Unix-likes it calls
// syscall.Exec, which replaces the current process image with the
// new binary while keeping the same PID. This is what makes the
// rollback transparent to anything supervising the launcher from
// above (launchd / systemd / docker).
type defaultExecer struct{}

func (defaultExecer) Exec(argv0 string, argv []string, envv []string) error {
	// syscall.Exec only returns on failure — on success it never
	// returns to Go at all.
	return syscall.Exec(argv0, argv, envv)
}
