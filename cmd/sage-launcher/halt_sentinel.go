// Package main — HALT sentinel handling for the v7.5 supervisor mode.
//
// The HALT sentinel is a small JSON file written by the supervised
// sage-gui binary when it determines it cannot safely continue and
// must be rolled back to a prior version. The sentinel itself is
// produced by the chain binary (see internal/snapshot + the deferred
// halt-handler in cmd/sage-gui/node.go, which is a separate later
// commit). The launcher's job is purely to detect, parse, and clear
// the sentinel — never to author it.
//
// Layout: ~/.sage/data/HALT  (a plain JSON file).
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// HaltSignal is the on-disk JSON payload the sage-gui binary writes
// to ~/.sage/data/HALT just before exiting non-zero to request a
// supervisor-driven rollback.
//
// The fields are stable wire-format between the chain binary and the
// launcher; do not rename them without coordinating a bump on both
// sides.
type HaltSignal struct {
	// FailedVersion is the binary version that determined it could
	// not continue (e.g. "v7.5.0").
	FailedVersion string `json:"failed_version"`

	// RollbackTo is the binary version the launcher should re-exec
	// into (e.g. "v7.1.0"). This must correspond to a snapshot
	// whose manifest.json declares BinaryVersion == RollbackTo and
	// whose <dir>/binary/ holds the matching executable.
	RollbackTo string `json:"rollback_to"`

	// FailureMessage is a free-form human-readable description of
	// why the chain halted. Surfaced to operators via the launcher
	// log and the GUI.
	FailureMessage string `json:"failure_message"`

	// Timestamp is the unix-epoch second when the sentinel was
	// written.
	Timestamp int64 `json:"timestamp"`
}

// ErrNoHaltSentinel is returned by ReadHaltSignal when the sentinel
// file does not exist. Callers use errors.Is to detect this so they
// can distinguish "no halt requested" from "halt requested but
// malformed".
var ErrNoHaltSentinel = errors.New("no HALT sentinel present")

// ReadHaltSignal reads and parses ~/.sage/data/HALT (or whichever
// path the caller hands in). Returns ErrNoHaltSentinel wrapped if
// the file is absent, or a parsing error if the JSON is malformed.
//
// The function performs minimal validation: FailedVersion must be
// non-empty. RollbackTo MAY be empty, in which case the caller is
// expected to resolve a rollback target by scanning available anchor
// snapshots (see findLatestRollbackAnchor in rollback.go) — this lets
// the chain binary write a HALT sentinel before it has any knowledge
// of which anchors exist on disk.
func ReadHaltSignal(path string) (*HaltSignal, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNoHaltSentinel, path)
		}
		return nil, fmt.Errorf("read HALT sentinel %s: %w", path, err)
	}

	var sig HaltSignal
	if err := json.Unmarshal(data, &sig); err != nil {
		return nil, fmt.Errorf("parse HALT sentinel %s: %w", path, err)
	}

	if sig.FailedVersion == "" {
		return nil, fmt.Errorf("HALT sentinel %s: failed_version is empty", path)
	}

	return &sig, nil
}

// ClearHaltSignal removes the sentinel file after a successful
// rollback launch. The launcher only calls this once the
// replacement binary has been verified to exist and the rollback
// flow has reached the "about to syscall.Exec" point — clearing
// earlier would lose state if the rollback itself failed.
//
// Returns nil if the file is already absent (idempotent).
func ClearHaltSignal(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear HALT sentinel %s: %w", path, err)
	}
	return nil
}
