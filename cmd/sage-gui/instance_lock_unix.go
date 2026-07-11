//go:build !windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"
)

const instanceLockFDEnv = "SAGE_INSTANCE_LOCK_FD"

type instanceLock struct{ file *os.File }

// acquireInstanceLock holds an OS advisory lock for the entire serve process.
// The descriptor is deliberately inherited across the coordinated syscall.Exec
// handoff, closing the health/PID restart gap where a second launcher could
// otherwise open the same stores and ports.
func acquireInstanceLock(sageHome string) (*instanceLock, error) {
	if err := os.MkdirAll(sageHome, 0700); err != nil {
		return nil, fmt.Errorf("create SAGE home for instance lock: %w", err)
	}
	path := filepath.Join(sageHome, "sage.instance.lock")
	if raw := os.Getenv(instanceLockFDEnv); raw != "" {
		fd, err := strconv.Atoi(raw)
		if err == nil && fd >= 3 {
			f := os.NewFile(uintptr(fd), "sage-instance.lock")
			if f != nil {
				fdInfo, fdStatErr := f.Stat()
				pathInfo, pathStatErr := os.Stat(path)
				if fdStatErr == nil && pathStatErr == nil && fdInfo.Mode().IsRegular() && os.SameFile(fdInfo, pathInfo) {
					if flockErr := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); flockErr != nil {
						return nil, fmt.Errorf("inherited descriptor does not own the SAGE instance lock: %w", flockErr)
					}
					if _, flagErr := unix.FcntlInt(f.Fd(), unix.F_SETFD, unix.FD_CLOEXEC); flagErr != nil {
						return nil, fmt.Errorf("protect inherited instance lock from child processes: %w", flagErr)
					}
					if unsetErr := os.Unsetenv(instanceLockFDEnv); unsetErr != nil {
						return nil, fmt.Errorf("clear inherited instance lock environment: %w", unsetErr)
					}
					return &instanceLock{file: f}, nil
				}
			}
		}
		return nil, fmt.Errorf("invalid inherited SAGE instance lock")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600) //nolint:gosec // fixed local runtime file
	if err != nil {
		return nil, fmt.Errorf("open instance lock: %w", err)
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another SAGE node already owns %s: %w", path, err)
	}
	return &instanceLock{file: f}, nil
}

// PrepareExec exposes the lock descriptor only for the immediate coordinated
// self-exec. During normal runtime it remains CLOEXEC, so Ollama/reranker and
// other child processes cannot accidentally keep the node lock alive.
func (l *instanceLock) PrepareExec() error {
	if l == nil || l.file == nil {
		return fmt.Errorf("instance lock unavailable")
	}
	if _, err := unix.FcntlInt(l.file.Fd(), unix.F_SETFD, 0); err != nil {
		return err
	}
	return os.Setenv(instanceLockFDEnv, strconv.FormatUint(uint64(l.file.Fd()), 10))
}

func (l *instanceLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	return l.file.Close()
}
