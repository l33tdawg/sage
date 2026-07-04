//go:build windows

package rerankd

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func sidecarSysProcAttr() *syscall.SysProcAttr {
	// CREATE_NEW_PROCESS_GROUP so Ctrl+C aimed at the node console doesn't
	// propagate to the sidecar.
	return &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func killSidecar(pid int) {
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
	}
}

// pidIsLlamaServer reports whether pid is still our llama-server process, so an
// adopted-sidecar Stop can't kill a recycled, unrelated pid. tasklist prints the
// image name for the given pid; we match it against the platform binary name.
func pidIsLlamaServer(pid int) bool {
	out, err := exec.Command("tasklist", "/FI", "PID eq "+strconv.Itoa(pid), "/FO", "CSV", "/NH").Output()
	if err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(string(out)), strings.ToLower(serverBinaryName()))
}
