//go:build !windows

package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"
)

const parentPIDEnv = "SAGE_PARENT_PID"

// startParentWatchdog stops a node whose launching tray has disappeared.
//
// macOS Force Quit does not run the tray's applicationWillTerminate hook, so
// the tray cannot terminate its child during a force quit. The orphaned node
// then keeps the single-instance lock (and port 8080), and the next tray
// launch fails with errInstanceLockHeld. Polling the parent relationship makes
// the child responsible for its own liveness instead of relying on a cleanup
// callback the OS may never invoke.
//
// The env var is only set by the tray. A node started directly from a terminal
// or by a launcher/supervisor has no watchdog.
func startParentWatchdog() {
	raw := os.Getenv(parentPIDEnv)
	if raw == "" {
		return
	}
	parentPID, err := strconv.Atoi(raw)
	if err != nil || parentPID <= 1 || parentPID == os.Getpid() {
		return
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// A force-quit tray reparents the node to launchd (PID 1). The
			// kill(0) check also catches a parent that is no longer reachable.
			if os.Getppid() != parentPID {
				fmt.Fprintf(os.Stderr, "SAGE: parent tray %d exited; stopping orphaned node\n", parentPID)
				_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
				return
			}
			if err := syscall.Kill(parentPID, 0); err != nil && !errors.Is(err, syscall.EPERM) {
				fmt.Fprintf(os.Stderr, "SAGE: parent tray %d is gone; stopping orphaned node\n", parentPID)
				_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
				return
			}
		}
	}()
}
