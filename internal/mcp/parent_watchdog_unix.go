//go:build !windows

package mcp

import (
	"fmt"
	"os"
)

type unixMCPParentProcess struct {
	pid int
}

func newMCPParentProcess(pid int) (mcpParentProcess, error) {
	if pid < 0 {
		return nil, fmt.Errorf("launching parent PID is unavailable: %d", pid)
	}
	return unixMCPParentProcess{pid: pid}, nil
}

func (parent unixMCPParentProcess) exited() bool {
	// Reparenting records the original parent's exit even if its PID has been
	// reused. Initial PID 0/1 is conservative: the client may be outside a PID
	// namespace or a container init, or this bridge may have been orphaned
	// before Run began. None proves the captured parent has exited.
	return mcpParentPIDChanged(parent.pid, os.Getppid())
}

func (unixMCPParentProcess) close() {}

func mcpParentPIDChanged(initial, current int) bool {
	return initial > 0 && current > 0 && initial != current
}
