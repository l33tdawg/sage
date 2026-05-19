// Package main — rollback flow for the v7.5 supervisor mode.
//
// When the supervised sage-gui binary writes a HALT sentinel and
// exits non-zero, the launcher walks ~/.sage/snapshots/ newest-first
// to find an anchor snapshot pinned to the requested rollback
// version, asks the injected Restorer to restore that snapshot
// into the data dir, and finally re-execs into the previous-version
// binary that travels inside the snapshot under binary/.
//
// The Restorer is an interface (not a direct dependency on
// internal/snapshot) so this package can be built and tested in
// isolation. The real implementation lives in internal/snapshot,
// which is being authored by a parallel agent. Wiring those
// together is an integration commit done later.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

// Restorer applies a snapshot directory's contents into a SAGE
// data directory. The supervisor calls it with the absolute path to
// the snapshot dir (e.g. ~/.sage/snapshots/12345) and the absolute
// path to the data dir (e.g. ~/.sage/data).
//
// Implementations are expected to be idempotent — partial restores
// from a prior failed attempt should be detectable and either
// recovered from or rejected cleanly.
type Restorer interface {
	Restore(snapshotDir, dataDir string) error
}

// nopRestorer is the default implementation used until
// internal/snapshot lands. It only logs what it would do; the real
// integration commit swaps this for the wired-in implementation.
type nopRestorer struct {
	logf func(format string, args ...interface{})
}

func (n *nopRestorer) Restore(snapshotDir, dataDir string) error {
	if n.logf != nil {
		n.logf("Restorer(stub): would restore %s -> %s", snapshotDir, dataDir)
	}
	return nil
}

// Execer replaces the running launcher process with the rollback
// binary. The default implementation calls syscall.Exec (see
// exec_unix.go / exec_windows.go). Tests inject a stub so they can
// assert the call happened without actually replacing the test
// runner process.
type Execer interface {
	Exec(argv0 string, argv []string, envv []string) error
}

// snapshotManifest is the subset of manifest.json the launcher
// cares about. The full schema is owned by internal/snapshot; we
// duplicate the two fields we need rather than importing that
// package (see top-of-file comment).
type snapshotManifest struct {
	BinaryVersion string `json:"binary_version"`
	Height        int64  `json:"height"`
	TakenAt       int64  `json:"taken_at"`
}

// RollbackContext is everything the rollback flow needs. It's a
// struct rather than a long argv so tests can construct one with
// stubs without threading parameters through three layers.
type RollbackContext struct {
	// SnapshotsDir is the directory holding numbered snapshot
	// subdirs (e.g. ~/.sage/snapshots).
	SnapshotsDir string

	// DataDir is the chain data dir to restore into
	// (e.g. ~/.sage/data).
	DataDir string

	// HaltPath is the path to the HALT sentinel file the launcher
	// just observed. Cleared on successful rollback launch.
	HaltPath string

	// LauncherLog is the path to the launcher's append-only event
	// log (e.g. ~/.sage/launcher.log). Used to surface the
	// rollback event to operators and out-of-process listeners.
	LauncherLog string

	// Restorer applies the chosen snapshot to DataDir. Defaults to
	// nopRestorer if nil.
	Restorer Restorer

	// Execer replaces the launcher process with the rollback
	// binary. Defaults to the real syscall.Exec wrapper if nil.
	Execer Execer

	// Now is a clock injection point for test determinism.
	Now func() time.Time

	// Logf prints diagnostic messages. Defaults to fmt.Printf-style
	// stderr.
	Logf func(format string, args ...interface{})
}

// HandleHalt runs the full rollback sequence. On success it does
// not return — the process is replaced by the rollback binary. It
// returns an error only when the flow cannot proceed (snapshot
// missing, binary missing, exec failure).
func HandleHalt(ctx RollbackContext, sig *HaltSignal) error {
	if ctx.Logf == nil {
		ctx.Logf = func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stderr, format+"\n", args...)
		}
	}
	if ctx.Now == nil {
		ctx.Now = time.Now
	}
	if ctx.Restorer == nil {
		ctx.Restorer = &nopRestorer{logf: ctx.Logf}
	}
	if ctx.Execer == nil {
		ctx.Execer = defaultExecer{}
	}

	ctx.Logf("ROLLBACK_TRIGGERED failed=%s rollback_to=%s reason=%q",
		sig.FailedVersion, sig.RollbackTo, sig.FailureMessage)

	var (
		snapDir  string
		manifest *snapshotManifest
		err      error
	)
	if sig.RollbackTo != "" {
		snapDir, manifest, err = findAnchorSnapshot(ctx.SnapshotsDir, sig.RollbackTo)
		if err != nil {
			return fmt.Errorf("locate anchor snapshot for %s: %w", sig.RollbackTo, err)
		}
	} else {
		// Empty rollback_to: chain binary didn't pick a target (e.g.
		// it crashed before it could enumerate available anchors).
		// Pick the newest anchor whose BinaryVersion differs from
		// the failed version — that's "the last known-good code".
		snapDir, manifest, err = findLatestRollbackAnchor(ctx.SnapshotsDir, sig.FailedVersion)
		if err != nil {
			return fmt.Errorf("locate fallback anchor (exclude %s): %w", sig.FailedVersion, err)
		}
		ctx.Logf("rollback_to was empty; selected latest anchor binary_version=%s", manifest.BinaryVersion)
	}
	ctx.Logf("rollback snapshot selected: %s (height=%d binary_version=%s)",
		snapDir, manifest.Height, manifest.BinaryVersion)

	binaryName := "sage-gui-" + manifest.BinaryVersion
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	rollbackBinary := filepath.Join(snapDir, "binary", binaryName)
	if _, statErr := os.Stat(rollbackBinary); statErr != nil {
		return fmt.Errorf("rollback binary missing at %s: %w", rollbackBinary, statErr)
	}

	// Apply the snapshot. The injected restorer is the boundary
	// with internal/snapshot — if anything goes wrong during real
	// restore the launcher surfaces it and refuses to re-exec
	// rather than booting against half-restored state.
	if restoreErr := ctx.Restorer.Restore(snapDir, ctx.DataDir); restoreErr != nil {
		return fmt.Errorf("restore snapshot %s: %w", snapDir, restoreErr)
	}
	ctx.Logf("snapshot %s restored into %s", filepath.Base(snapDir), ctx.DataDir)

	// Surface the event to GUI/CLI/voice-bridge listeners. Best
	// effort — log failures shouldn't block the actual rollback.
	if ctx.LauncherLog != "" {
		if logErr := appendRollbackEvent(ctx.LauncherLog, sig, snapDir, ctx.Now()); logErr != nil {
			ctx.Logf("warn: append launcher.log: %v", logErr)
		}
	}

	// Clear the sentinel only AFTER we know the binary exists and
	// the restore completed. If we cleared earlier and the exec
	// below fails, the next boot would have no record of the halt.
	if clearErr := ClearHaltSignal(ctx.HaltPath); clearErr != nil {
		ctx.Logf("warn: clear HALT sentinel: %v", clearErr)
	}

	// Re-exec into the rollback binary, replacing this launcher
	// process. We deliberately use syscall.Exec rather than
	// os/exec.Cmd: the launcher's PID becomes the rollback binary,
	// so anything supervising the launcher from above (systemd,
	// launchd, etc.) keeps the same PID and never sees a transient
	// "no process" gap. Future maintainers: do NOT "fix" this to
	// os/exec.Cmd — that creates a parent/child split and breaks
	// outer-layer supervision.
	argv := []string{rollbackBinary, "serve"}
	env := os.Environ()
	ctx.Logf("exec rollback binary: %s %v", rollbackBinary, argv[1:])
	if execErr := ctx.Execer.Exec(rollbackBinary, argv, env); execErr != nil {
		return fmt.Errorf("exec rollback binary %s: %w", rollbackBinary, execErr)
	}

	// Unreachable on real syscall.Exec; reachable in tests that
	// inject a stub Execer that returns nil without exec'ing.
	return nil
}

// findAnchorSnapshot scans the snapshots directory newest-first and
// returns the first one whose manifest.json declares
// BinaryVersion == rollbackTo and whose OK sentinel exists.
//
// Newest-first matches the design doc: the most recent anchor for
// the target version is the smallest data delta to replay forward
// after the rollback. We rely on snapshot directory names being
// monotonic height strings — same convention the snapshot package
// uses (~/.sage/snapshots/<height>/).
func findAnchorSnapshot(snapshotsDir, rollbackTo string) (string, *snapshotManifest, error) {
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		return "", nil, fmt.Errorf("read snapshots dir %s: %w", snapshotsDir, err)
	}

	// Filter to directories, skip staging dirs (".staging-*"), sort
	// newest-first by name. Names are heights so lexical-desc on
	// zero-padded heights would be ideal, but the writer uses
	// %d not %09d — fall back to numeric comparison via mtime,
	// then name.
	type candidate struct {
		name  string
		mtime time.Time
	}
	var candidates []candidate
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) >= 1 && name[0] == '.' {
			continue
		}
		info, infoErr := e.Info()
		if infoErr != nil {
			continue
		}
		candidates = append(candidates, candidate{name: name, mtime: info.ModTime()})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].mtime.After(candidates[j].mtime)
	})

	var lastErr error
	for _, c := range candidates {
		dir := filepath.Join(snapshotsDir, c.name)

		// Skip dirs without OK sentinel — they're either staging
		// or were marked corrupt by the verifier.
		if _, statErr := os.Stat(filepath.Join(dir, "OK")); statErr != nil {
			continue
		}

		manifestPath := filepath.Join(dir, "manifest.json")
		raw, readErr := os.ReadFile(manifestPath)
		if readErr != nil {
			lastErr = fmt.Errorf("read %s: %w", manifestPath, readErr)
			continue
		}
		var m snapshotManifest
		if jsonErr := json.Unmarshal(raw, &m); jsonErr != nil {
			lastErr = fmt.Errorf("parse %s: %w", manifestPath, jsonErr)
			continue
		}
		if m.BinaryVersion == rollbackTo {
			return dir, &m, nil
		}
	}

	if lastErr != nil {
		return "", nil, fmt.Errorf("no anchor snapshot for %s (last parse error: %v)", rollbackTo, lastErr)
	}
	return "", nil, fmt.Errorf("no anchor snapshot for %s in %s", rollbackTo, snapshotsDir)
}

// findLatestRollbackAnchor scans snapshots newest-first and returns
// the first whose BinaryVersion is non-empty AND not equal to
// excludeVersion. Used when the HALT sentinel didn't specify a
// rollback target — we pick "anything but the version that just
// failed". Matches findAnchorSnapshot's directory layout and
// OK-sentinel gating.
func findLatestRollbackAnchor(snapshotsDir, excludeVersion string) (string, *snapshotManifest, error) {
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		return "", nil, fmt.Errorf("read snapshots dir %s: %w", snapshotsDir, err)
	}

	type candidate struct {
		name  string
		mtime time.Time
	}
	var candidates []candidate
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) >= 1 && name[0] == '.' {
			continue
		}
		info, infoErr := e.Info()
		if infoErr != nil {
			continue
		}
		candidates = append(candidates, candidate{name: name, mtime: info.ModTime()})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].mtime.After(candidates[j].mtime)
	})

	var lastErr error
	for _, c := range candidates {
		dir := filepath.Join(snapshotsDir, c.name)
		if _, statErr := os.Stat(filepath.Join(dir, "OK")); statErr != nil {
			continue
		}
		raw, readErr := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if readErr != nil {
			lastErr = readErr
			continue
		}
		var m snapshotManifest
		if jsonErr := json.Unmarshal(raw, &m); jsonErr != nil {
			lastErr = jsonErr
			continue
		}
		if m.BinaryVersion == "" || m.BinaryVersion == excludeVersion {
			continue
		}
		return dir, &m, nil
	}

	if lastErr != nil {
		return "", nil, fmt.Errorf("no anchor snapshot excluding %s (last parse error: %v)", excludeVersion, lastErr)
	}
	return "", nil, fmt.Errorf("no anchor snapshot excluding %s in %s", excludeVersion, snapshotsDir)
}

// rollbackEvent is the JSON record appended to launcher.log when a
// rollback fires. The schema is intentionally small and stable so
// downstream listeners (GUI, voice-bridge) can tail-parse cheaply.
type rollbackEvent struct {
	Event          string `json:"event"`
	Timestamp      int64  `json:"timestamp"`
	FailedVersion  string `json:"failed_version"`
	RollbackTo     string `json:"rollback_to"`
	FailureMessage string `json:"failure_message"`
	SnapshotDir    string `json:"snapshot_dir"`
}

func appendRollbackEvent(logPath string, sig *HaltSignal, snapDir string, now time.Time) error {
	if mkErr := os.MkdirAll(filepath.Dir(logPath), 0o755); mkErr != nil {
		return mkErr
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	evt := rollbackEvent{
		Event:          "ROLLBACK_TRIGGERED",
		Timestamp:      now.Unix(),
		FailedVersion:  sig.FailedVersion,
		RollbackTo:     sig.RollbackTo,
		FailureMessage: sig.FailureMessage,
		SnapshotDir:    snapDir,
	}
	line, mErr := json.Marshal(&evt)
	if mErr != nil {
		return mErr
	}
	if _, wErr := f.Write(append(line, '\n')); wErr != nil {
		return wErr
	}
	return nil
}

// ErrRollbackBinaryMissing is exported for tests that assert the
// launcher correctly errors when the rollback binary is absent.
var ErrRollbackBinaryMissing = errors.New("rollback binary missing")
