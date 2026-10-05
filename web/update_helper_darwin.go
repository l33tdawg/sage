//go:build darwin

package web

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
)

const sageUpdateHelperName = "sage-update-helper"

func pendingAppStartupMarker(execPath string) string {
	bundle := macOSAppBundleForExecutable(execPath)
	if bundle == "" {
		return execPath + ".update-started"
	}
	return bundle + ".update-started"
}

func pendingAppExternalHelper(execPath string) string {
	bundle := macOSAppBundleForExecutable(execPath)
	if bundle == "" {
		return execPath + ".update-helper"
	}
	return bundle + ".update-helper"
}

func markPendingUpdateStartup(execPath string) error {
	version := PendingUpdateVersion(execPath)
	if version == "" {
		return nil
	}
	return writeFileAtomicDurable(pendingAppStartupMarker(execPath), []byte(version+"\n"), 0600)
}

func platformStartPendingUpdateRecovery(execPath string) (func(), func(), error) {
	if PendingUpdateVersion(execPath) == "" || macOSAppBundleForExecutable(execPath) == "" {
		return func() {}, func() {}, nil
	}
	helperPath := pendingAppExternalHelper(execPath)
	if _, err := requireUpdateBinaryFile(helperPath, "external update recovery helper"); err != nil {
		return nil, nil, err
	}
	if err := verifySignedSAGELeaf(context.Background(), helperPath, sageUpdateHelperName); err != nil {
		return nil, nil, fmt.Errorf("verify external update recovery helper: %w", err)
	}
	if err := removeFileDurable(pendingAppStartupMarker(execPath)); err != nil && !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("clear prior update startup guard: %w", err)
	}
	// Deliberately not exec.CommandContext: the helper is setsid-detached so it
	// survives the node it is about to replace. A context here would cancel the
	// updater at exactly the moment it must keep running.
	//nolint:noctx // setsid-detached helper must survive the node it replaces
	cmd := exec.Command(helperPath, "watch", "--pid", strconv.Itoa(os.Getpid()), "--exec", execPath) // #nosec G204 -- exact updater-owned signed helper and executable
	cmd.Dir = filepath.Dir(helperPath)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("start external update recovery helper: %w", err)
	}
	var finish sync.Once
	commit := func() {
		finish.Do(func() { _ = cmd.Process.Release() })
	}
	abort := func() {
		finish.Do(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		})
	}
	return commit, abort, nil
}
