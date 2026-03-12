//go:build windows

package main

import (
	"os"
	"os/exec"
	"syscall"
)

// setSysProcAttr configures the child process to run hidden on Windows
// (no console window flash) and in its own process group.
func setSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}
}

// processAlive checks if a process with the given PID exists on Windows.
func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Windows, FindProcess only succeeds for existing processes.
	// Send signal 0 to verify — returns nil if process exists.
	err = proc.Signal(syscall.Signal(0))
	return err == nil
}
