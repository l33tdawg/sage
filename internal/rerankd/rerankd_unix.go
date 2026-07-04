//go:build !windows

package rerankd

import (
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// sidecarSysProcAttr puts the sidecar in its own process group so a signal
// aimed at the SAGE node's group doesn't take the sidecar down accidentally,
// and so killSidecar can terminate the whole group deliberately.
func sidecarSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// killSidecar terminates the sidecar's process group (falling back to the
// single pid when it leads no group).
func killSidecar(pid int) {
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
}

// pidIsLlamaServer reports whether pid is still our llama-server process, so an
// adopted-sidecar Stop can't SIGTERM a recycled, unrelated pid. `ps -o comm=`
// prints the process's command basename on both macOS and Linux.
func pidIsLlamaServer(pid int) bool {
	out, err := exec.Command("ps", "-o", "comm=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return false
	}
	return filepath.Base(strings.TrimSpace(string(out))) == binaryName
}
