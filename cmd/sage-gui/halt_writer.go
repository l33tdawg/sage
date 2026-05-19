// Package main — v7.5 HALT sentinel writer.
//
// When the chain binary determines it cannot continue (consensus
// handler panic, post-upgrade migration failure, etc.), it writes a
// JSON sentinel to <DataDir>/HALT and exits non-zero. The launcher
// running in --supervise mode reads the sentinel, scans available
// anchor snapshots, and re-execs into the rollback binary.
//
// Wire-format contract: this file is the producer side. The
// consumer side lives in cmd/sage-launcher/halt_sentinel.go.
// Field names are stable; do not rename without coordinating both
// sides.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// haltSignal mirrors cmd/sage-launcher/halt_sentinel.go HaltSignal.
// We do not import the launcher package here (sage-gui must not
// depend on the launcher; the launcher already depends on sage-gui
// state). Keeping a structurally identical local copy is the
// intended boundary.
type haltSignal struct {
	FailedVersion  string `json:"failed_version"`
	RollbackTo     string `json:"rollback_to"`
	FailureMessage string `json:"failure_message"`
	Timestamp      int64  `json:"timestamp"`
}

// writeHaltSentinel persists a HALT sentinel atomically into
// <dataDir>/HALT. rollbackTo MAY be empty — the launcher's
// findLatestRollbackAnchor handles that case by picking "any
// anchor whose BinaryVersion isn't the failed version". This lets
// the chain binary signal "halt me" without having to enumerate
// available anchors at panic time.
//
// Atomicity: write to <dataDir>/.HALT.tmp first, fsync, then
// os.Rename to <dataDir>/HALT. A crash between write and rename
// leaves no sentinel; the supervisor will treat the crash as a
// regular non-zero exit and apply the crash-loop circuit-breaker
// rather than triggering rollback. Acceptable failure mode —
// rollback without a sentinel could be wrong.
func writeHaltSentinel(dataDir, failedVersion, rollbackTo, failureMsg string) error {
	if dataDir == "" {
		return fmt.Errorf("writeHaltSentinel: empty dataDir")
	}
	if failedVersion == "" {
		return fmt.Errorf("writeHaltSentinel: empty failedVersion")
	}

	sig := haltSignal{
		FailedVersion:  failedVersion,
		RollbackTo:     rollbackTo,
		FailureMessage: failureMsg,
		Timestamp:      time.Now().Unix(),
	}
	payload, err := json.MarshalIndent(sig, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal HALT sentinel: %w", err)
	}

	if mkErr := os.MkdirAll(dataDir, 0o700); mkErr != nil {
		return fmt.Errorf("mkdir %s: %w", dataDir, mkErr)
	}

	tmp := filepath.Join(dataDir, ".HALT.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if _, err := f.Write(payload); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}

	final := filepath.Join(dataDir, "HALT")
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, final, err)
	}
	return nil
}

// haltOnPanic recovers a panic at the top of runServe, writes a
// HALT sentinel, then re-panics so the binary still exits non-zero.
// Deferred from runServe; the deferred close+exit chain runs normally
// before main() picks up the non-zero err.
//
// Captures the panic message into FailureMessage so the launcher's
// rollback log records what actually broke. RollbackTo is left
// empty — the launcher will pick the latest non-failed anchor.
func haltOnPanic(dataDir string) {
	r := recover()
	if r == nil {
		return
	}
	failureMsg := fmt.Sprintf("panic: %v", r)
	if err := writeHaltSentinel(dataDir, version, "", failureMsg); err != nil {
		// We're already mid-panic; the best we can do is print and
		// re-panic so the original stack reaches stderr.
		fmt.Fprintf(os.Stderr, "halt_writer: failed to write HALT sentinel: %v\n", err)
	}
	// Re-panic so the original stack trace surfaces in launcher.log
	// and the process exits non-zero. The deferred close calls in
	// runServe still run on the panic unwind.
	panic(r)
}
