//go:build darwin

// sage-update-helper is a separately signed, outside-the-app recovery owner.
// It restores the preserved app only when exec never entered the replacement
// binary. Once replacement startup is observed, downgrade is permanently off.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/l33tdawg/sage/web"
)

const replacementStartupGrace = 30 * time.Second

func main() {
	if len(os.Args) < 2 || os.Args[1] != "watch" {
		fmt.Fprintln(os.Stderr, "usage: sage-update-helper watch --pid PID --exec PATH")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("watch", flag.ContinueOnError)
	pid := flags.Int("pid", 0, "process ID being replaced")
	execPath := flags.String("exec", "", "installed sage-gui path")
	if err := flags.Parse(os.Args[2:]); err != nil || *pid < 2 || *execPath == "" || !filepath.IsAbs(*execPath) {
		os.Exit(2)
	}
	// Derive from the exact canonical bundle layout rather than accepting an
	// arbitrary deletion/rollback target from the command line.
	bundle, ok := bundleForExecutable(*execPath)
	if !ok {
		os.Exit(2)
	}
	startupMarker := bundle + ".update-started"
	for {
		if _, err := os.Lstat(startupMarker); err == nil {
			return
		} else if !os.IsNotExist(err) {
			return
		}
		if err := syscall.Kill(*pid, 0); err == nil || errors.Is(err, syscall.EPERM) {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		// The replacement cannot create its startup marker until the old process
		// releases the single-instance lock and launchd/the tray starts the new app.
		// Give that normal handoff a bounded window before deciding launch failed.
		deadline := time.Now().Add(replacementStartupGrace)
		for time.Now().Before(deadline) {
			if _, err := os.Lstat(startupMarker); err == nil {
				return
			} else if !os.IsNotExist(err) {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
		rolledBack, err := web.RollbackPendingUpdate(*execPath)
		if err != nil || !rolledBack {
			return
		}
		// Deliberately not exec.CommandContext: this launcher must outlive the
		// caller, which is exiting precisely so the bundle can be replaced.
		// Binding it to a context would kill the relaunch mid-update.
		//nolint:noctx // must outlive the caller, which is exiting to let the bundle be replaced
		_ = exec.Command("/usr/bin/open", bundle).Start() // #nosec G204 -- fixed launcher and derived app path
		return
	}
}

func bundleForExecutable(execPath string) (string, bool) {
	clean := filepath.Clean(execPath)
	bundle := filepath.Dir(filepath.Dir(filepath.Dir(clean)))
	if !strings.HasSuffix(strings.ToLower(filepath.Base(bundle)), ".app") ||
		filepath.Join(bundle, "Contents", "MacOS", "sage-gui") != clean {
		return "", false
	}
	return bundle, true
}
