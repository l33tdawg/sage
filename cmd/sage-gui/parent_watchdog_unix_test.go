//go:build !windows

package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// TestParentWatchdogHelper is the child half of the regression test below. It
// is skipped during normal test runs and only runs when the parent launches it
// with SAGE_PARENT_WATCHDOG_HELPER=1.
func TestParentWatchdogHelper(t *testing.T) {
	if os.Getenv("SAGE_PARENT_WATCHDOG_HELPER") != "1" {
		t.Skip("helper process only")
	}
	marker := os.Getenv("SAGE_PARENT_WATCHDOG_MARKER")
	if marker == "" {
		os.Exit(2)
	}

	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGTERM)
	startParentWatchdog()
	if os.Getenv("SAGE_PARENT_WATCHDOG_EXPECT_ALIVE") == "1" {
		select {
		case <-stopped:
			os.Exit(5)
		case <-time.After(1500 * time.Millisecond):
			os.Exit(0)
		}
	}

	select {
	case <-stopped:
		if err := os.WriteFile(marker, []byte("watchdog stopped the orphan"), 0600); err != nil {
			os.Exit(3)
		}
		os.Exit(0)
	case <-time.After(10 * time.Second):
		os.Exit(4)
	}
}

func TestParentWatchdogStopsNodeWhenLaunchingParentExits(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "watchdog-fired")
	// The shell exits immediately after backgrounding the helper, reparenting
	// it to launchd. SAGE_PARENT_PID deliberately names the now-dead shell;
	// this is the same relationship a force-quit tray leaves behind.
	command := fmt.Sprintf(
		`SAGE_PARENT_PID=$$ SAGE_PARENT_WATCHDOG_HELPER=1 SAGE_PARENT_WATCHDOG_MARKER=%q %q -test.run=^TestParentWatchdogHelper$ &`,
		marker, os.Args[0],
	)
	if err := exec.Command("/bin/sh", "-c", command).Run(); err != nil {
		t.Fatalf("launch watchdog helper: %v", err)
	}

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("watchdog did not stop the node after its launching parent exited")
}

func TestParentWatchdogLeavesLiveParentAndStandaloneNodesRunning(t *testing.T) {
	for _, pid := range []string{strconv.Itoa(os.Getpid()), "", "invalid", "1"} {
		t.Run("parent="+pid, func(t *testing.T) {
			child := exec.Command(os.Args[0], "-test.run=^TestParentWatchdogHelper$")
			child.Env = append(os.Environ(), "SAGE_PARENT_PID="+pid,
				"SAGE_PARENT_WATCHDOG_HELPER=1", "SAGE_PARENT_WATCHDOG_EXPECT_ALIVE=1",
				"SAGE_PARENT_WATCHDOG_MARKER="+filepath.Join(t.TempDir(), "unused"))
			if out, err := child.CombinedOutput(); err != nil {
				t.Fatalf("watchdog stopped a live/standalone child: %v %s", err, out)
			}
		})
	}
}
