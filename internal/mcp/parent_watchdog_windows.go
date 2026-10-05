//go:build windows

package mcp

import (
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

type windowsMCPParentProcess struct {
	handle windows.Handle
	gone   bool
}

func newMCPParentProcess(pid int) (mcpParentProcess, error) {
	if pid <= 0 {
		return nil, fmt.Errorf("launching parent PID is unavailable: %d", pid)
	}
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		return windowsMCPParentProcess{gone: true}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open launching parent process %d: %w", pid, err)
	}
	var parentCreated, childCreated, exited, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(handle, &parentCreated, &exited, &kernel, &user); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("read launching parent creation time: %w", err)
	}
	if err := windows.GetProcessTimes(windows.CurrentProcess(), &childCreated, &exited, &kernel, &user); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("read stdio bridge creation time: %w", err)
	}
	// Getppid on Windows returns the creator PID even after it exits. If that
	// PID was reused before we acquired the handle, the newer process cannot
	// be our parent. Once captured, the handle protects against later reuse.
	return windowsMCPParentProcess{handle: handle, gone: parentCreated.Nanoseconds() > childCreated.Nanoseconds()}, nil
}

func (parent windowsMCPParentProcess) exited() bool {
	if parent.gone {
		return true
	}
	state, err := windows.WaitForSingleObject(parent.handle, 0)
	return err == nil && state == windows.WAIT_OBJECT_0
}

func (parent windowsMCPParentProcess) close() {
	if parent.handle != 0 {
		_ = windows.CloseHandle(parent.handle)
	}
}
