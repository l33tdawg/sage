//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// setSysProcAttr sets Unix process attributes so the child process
// runs in its own session and survives the launcher's exit.
func setSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}
}

// processAlive checks if a process with the given PID is alive on Unix
// by sending signal 0.
func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}
