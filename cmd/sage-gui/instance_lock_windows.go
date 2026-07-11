//go:build windows

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

type instanceLock struct {
	file       *os.File
	overlapped windows.Overlapped
}

func acquireInstanceLock(sageHome string) (*instanceLock, error) {
	if err := os.MkdirAll(sageHome, 0700); err != nil {
		return nil, fmt.Errorf("create SAGE home for instance lock: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(sageHome, "sage.instance.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open instance lock: %w", err)
	}
	l := &instanceLock{file: f}
	err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &l.overlapped)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another SAGE node is already running: %w", err)
	}
	return l, nil
}

func (l *instanceLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = windows.UnlockFileEx(windows.Handle(l.file.Fd()), 0, 1, 0, &l.overlapped)
	return l.file.Close()
}

func (l *instanceLock) PrepareExec() error { return nil }
