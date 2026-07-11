// Package main — v7.5 supervisor mode.
//
// The supervisor runs the sage-gui binary in the foreground (not
// detached), pipes stdout/stderr through, forwards SIGINT/SIGTERM,
// and inspects the child's exit conditions:
//
//   - exit 0, no HALT  -> propagate exit 0, stop.
//   - non-zero, no HALT -> crash. Restart with backoff up to N=3 in
//     a 60s window. Beyond that, log fatal and exit.
//   - HALT sentinel present (any exit code) -> read sentinel, run
//     rollback flow (see rollback.go). HandleHalt should not return
//     on success; if it does (test stub or error) we propagate.
//
// The supervisor is opt-in via the `--supervise` flag. The existing
// "fire and forget + open browser" behavior remains the default so
// the macOS .app / Windows .exe double-click flow is preserved.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// SupervisorConfig is everything the supervisor needs. All paths
// are absolute. Tests build one against t.TempDir(); the production
// entrypoint builds one from sageDir() + findSageGUI().
type SupervisorConfig struct {
	// BinaryPath is the sage-gui executable to run as the child.
	BinaryPath string

	// BinaryArgs is the argv after the binary name. Defaults to
	// ["serve"] in the production path; tests use a fake-child
	// argv that exits in a controlled way.
	BinaryArgs []string

	// SageHome is the SAGE home directory (e.g. ~/.sage). Used to
	// locate sage.pid, launcher.log, snapshots/, and data/HALT.
	SageHome string

	// DataDir is the chain data dir. The HALT sentinel lives at
	// <DataDir>/HALT.
	DataDir string

	// SnapshotsDir is the directory holding numbered snapshot
	// subdirs (e.g. <SageHome>/snapshots).
	SnapshotsDir string

	// LauncherLog is where ROLLBACK_TRIGGERED events are appended.
	LauncherLog string

	// MaxCrashes is the crash-loop circuit-breaker threshold.
	// Restarts beyond this within CrashWindow cause the
	// supervisor to give up and exit non-zero. Defaults to 3.
	MaxCrashes int

	// CrashWindow is the sliding window over which MaxCrashes is
	// counted. Defaults to 60 seconds.
	CrashWindow time.Duration

	// RestartDelay is the wall-clock pause between crash restarts.
	// Defaults to 1 second; tests set it to 0 for speed.
	RestartDelay time.Duration

	// Restorer is the dependency-injected snapshot applier.
	// Defaults to a stub that only logs. The real implementation
	// lives in internal/snapshot and is wired in by a separate
	// integration commit.
	Restorer Restorer

	// Execer replaces the launcher process with the rollback
	// binary. Defaults to syscall.Exec; tests inject a stub.
	Execer Execer

	// Stdout / Stderr are where the child's piped output goes.
	// Default to os.Stdout / os.Stderr.
	Stdout io.Writer
	Stderr io.Writer

	// Logf prints supervisor-level events. Defaults to stderr.
	Logf func(format string, args ...interface{})

	// Now is the clock injection point.
	Now func() time.Time
}

func (c *SupervisorConfig) applyDefaults() {
	if c.MaxCrashes == 0 {
		c.MaxCrashes = 3
	}
	if c.CrashWindow == 0 {
		c.CrashWindow = 60 * time.Second
	}
	if c.RestartDelay == 0 {
		c.RestartDelay = 1 * time.Second
	}
	if c.Stdout == nil {
		c.Stdout = os.Stdout
	}
	if c.Stderr == nil {
		c.Stderr = os.Stderr
	}
	if c.Logf == nil {
		c.Logf = func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stderr, "[supervisor] "+format+"\n", args...)
		}
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if len(c.BinaryArgs) == 0 {
		c.BinaryArgs = []string{"serve"}
	}
}

// haltPath is the path the supervisor watches for the HALT
// sentinel. Centralized so tests and production agree.
func (c *SupervisorConfig) haltPath() string {
	return filepath.Join(c.DataDir, "HALT")
}

// pidFile is where we record the live child's PID so out-of-band
// callers (GUI, CLI) can find it. Cleared on healthy exit.
func (c *SupervisorConfig) pidFile() string {
	return filepath.Join(c.SageHome, "sage.pid")
}

// Run is the supervisor's main loop. It returns the exit code the
// launcher itself should propagate.
//
// The signature uses a context so callers (tests, an outer cmd) can
// cancel the supervisor cleanly. Production wires it to a
// signal-bound context.
func (c *SupervisorConfig) Run(ctx context.Context) int {
	c.applyDefaults()

	// crashTimes is a sliding window of recent child-crash times.
	// We append a timestamp on each non-zero non-HALT exit and
	// evict entries older than CrashWindow before each check.
	var crashTimes []time.Time

	for {
		if err := ctx.Err(); err != nil {
			c.Logf("supervisor context cancelled: %v", err)
			return 130 // conventional SIGINT exit code
		}

		exitCode, haltDetected, runErr := c.runOnce(ctx)
		if runErr != nil {
			c.Logf("supervisor run error: %v", runErr)
			return 1
		}

		// Halt path takes precedence over exit code: the binary
		// may exit cleanly after writing the sentinel.
		if haltDetected {
			sig, readErr := ReadHaltSignal(c.haltPath())
			if readErr != nil {
				// Malformed sentinel — refuse to act, surface
				// for operator intervention.
				c.Logf("HALT sentinel present but unreadable: %v", readErr)
				return 1
			}
			rbCtx := RollbackContext{
				SnapshotsDir: c.SnapshotsDir,
				DataDir:      c.DataDir,
				HaltPath:     c.haltPath(),
				LauncherLog:  c.LauncherLog,
				Restorer:     c.Restorer,
				Execer:       c.Execer,
				Logf:         c.Logf,
				Now:          c.Now,
			}
			if err := HandleHalt(rbCtx, sig); err != nil {
				c.Logf("rollback failed: %v", err)
				return 1
			}
			// HandleHalt only returns when Execer is a test stub.
			// In production it has already replaced our process.
			return 0
		}

		if exitCode == 0 {
			c.Logf("child exited cleanly")
			return 0
		}

		// Crash. Add to window, evict old entries, check
		// circuit breaker.
		now := c.Now()
		crashTimes = append(crashTimes, now)
		cutoff := now.Add(-c.CrashWindow)
		for len(crashTimes) > 0 && crashTimes[0].Before(cutoff) {
			crashTimes = crashTimes[1:]
		}
		if len(crashTimes) > c.MaxCrashes {
			c.Logf("crash-loop circuit-breaker tripped: %d crashes in %s — giving up",
				len(crashTimes), c.CrashWindow)
			return 1
		}
		c.Logf("child crashed (exit=%d), restart %d/%d within %s",
			exitCode, len(crashTimes), c.MaxCrashes, c.CrashWindow)

		// Restart delay; honor context cancellation.
		if c.RestartDelay > 0 {
			select {
			case <-ctx.Done():
				return 130
			case <-time.After(c.RestartDelay):
			}
		}
	}
}

// runOnce spawns a single child, waits for it, and returns its
// exit code and whether a HALT sentinel was found on exit.
//
// The boolean is checked AFTER the child has exited (not during
// run) so we don't act on a half-written sentinel. The writer is
// expected to fsync before exiting; the supervisor relies on this.
func (c *SupervisorConfig) runOnce(ctx context.Context) (exitCode int, haltDetected bool, err error) {
	// Clear any stale HALT from a previous boot before launching
	// the child. The chain binary writes a fresh sentinel on
	// failure; stale ones from a prior successful rollback would
	// cause a spurious rollback loop.
	if _, statErr := os.Stat(c.haltPath()); statErr == nil {
		// On supervisor start we treat an existing sentinel as
		// authoritative — caller should have already drained it.
		// We do NOT delete it here; instead, surface to the
		// caller by short-circuiting through the halt path.
		c.Logf("found pre-existing HALT sentinel at %s — handling without spawn", c.haltPath())
		return 0, true, nil
	}

	cmd := exec.CommandContext(ctx, c.BinaryPath, c.BinaryArgs...) //nolint:gosec // launcher invokes its own binary
	cmd.Stdout = c.Stdout
	cmd.Stderr = c.Stderr
	cmd.Env = os.Environ()
	// Run in the same process group so SIGINT/SIGTERM forwarded
	// to the supervisor reach the child too (the proc_other.go /
	// proc_windows.go helpers from the existing launcher are for
	// the detached path and intentionally do the opposite).

	if startErr := cmd.Start(); startErr != nil {
		return 0, false, fmt.Errorf("start %s: %w", c.BinaryPath, startErr)
	}

	// Best-effort pid file for out-of-band consumers.
	pidPath := c.pidFile()
	_ = os.MkdirAll(filepath.Dir(pidPath), 0o755)
	_ = os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)
	defer func() {
		if data, readErr := os.ReadFile(pidPath); readErr == nil && strings.TrimSpace(string(data)) == strconv.Itoa(cmd.Process.Pid) {
			_ = os.Remove(pidPath)
		}
	}()

	// Forward SIGINT/SIGTERM to the child. We do this in a
	// goroutine bound to the child's lifetime via stopCh.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case s := <-sigCh:
				if cmd.Process != nil {
					_ = cmd.Process.Signal(s)
				}
			case <-stopCh:
				signal.Stop(sigCh)
				return
			}
		}
	}()

	waitErr := cmd.Wait()
	close(stopCh)

	// Inspect HALT sentinel AFTER the child exited. The child
	// writes-and-fsyncs before exit, so by the time Wait()
	// returns the sentinel is durably on disk if it was going
	// to be there at all.
	if _, statErr := os.Stat(c.haltPath()); statErr == nil {
		haltDetected = true
	}

	if waitErr == nil {
		return 0, haltDetected, nil
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		return exitErr.ExitCode(), haltDetected, nil
	}
	// I/O error from Wait itself (rare). Treat as a crash with a
	// synthetic exit code.
	return -1, haltDetected, nil
}

// runSuperviseMode is the entrypoint invoked from main.go when the
// --supervise flag is set. It parses the remaining flags, builds a
// SupervisorConfig from the environment, and calls Run.
func runSuperviseMode(args []string) int {
	fs := flag.NewFlagSet("supervise", flag.ExitOnError)
	binaryFlag := fs.String("binary", "", "absolute path to sage-gui (default: auto-discover)")
	maxCrashes := fs.Int("max-crashes", 3, "crash-loop circuit-breaker threshold")
	crashWindow := fs.Duration("crash-window", 60*time.Second, "sliding window for crash counting")
	_ = fs.Parse(args)

	binaryPath := *binaryFlag
	if binaryPath == "" {
		binaryPath = findSageGUI()
	}
	if binaryPath == "" {
		fmt.Fprintln(os.Stderr, "supervisor: sage-gui binary not found")
		return 1
	}

	home := sageDir()
	dataDir := filepath.Join(home, "data")
	cfg := &SupervisorConfig{
		BinaryPath: binaryPath,
		BinaryArgs: []string{"serve"},
		SageHome:   home,
		DataDir:    dataDir,
		// SnapshotsDir lives INSIDE dataDir because internal/snapshot.Take
		// writes to dataDir/snapshots/ (see snapshotsDirName const). If
		// these paths drift apart the launcher will scan an empty
		// directory and fail rollback with "no anchor snapshot."
		SnapshotsDir: filepath.Join(dataDir, "snapshots"),
		LauncherLog:  filepath.Join(home, "launcher.log"),
		MaxCrashes:   *maxCrashes,
		CrashWindow:  *crashWindow,
		Restorer: &snapshotRestorer{
			logf: func(format string, args ...interface{}) {
				fmt.Fprintf(os.Stderr, "[supervise] "+format+"\n", args...)
			},
		},
	}

	ctx, cancel := signalContext(context.Background())
	defer cancel()
	return cfg.Run(ctx)
}

// signalContext returns a context that's cancelled on SIGINT /
// SIGTERM. Lives in supervisor.go (not proc_other.go) because it's
// platform-portable — signal.NotifyContext handles both Unix and
// Windows console signals.
func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
}
