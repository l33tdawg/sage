//go:build darwin

package rerankd

import (
	"runtime"
	"syscall"
)

// effectiveArch returns the HARDWARE architecture the sidecar should be
// built for. An amd64 SAGE binary translated by Rosetta reports GOARCH
// "amd64", but the machine is arm64 - and a separate child process can (and
// should) run the native arm64 build.
func effectiveArch() string {
	if runtime.GOARCH == "amd64" {
		if v, err := syscall.SysctlUint32("sysctl.proc_translated"); err == nil && v == 1 {
			return "arm64"
		}
	}
	return runtime.GOARCH
}
