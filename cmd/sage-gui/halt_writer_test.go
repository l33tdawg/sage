package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteHaltSentinel_AtomicAndParseable(t *testing.T) {
	dataDir := t.TempDir()
	if err := writeHaltSentinel(dataDir, "v7.5.0", "v7.1.0", "panic: simulated"); err != nil {
		t.Fatalf("writeHaltSentinel: %v", err)
	}

	haltPath := filepath.Join(dataDir, "HALT")
	raw, err := os.ReadFile(haltPath)
	if err != nil {
		t.Fatalf("read HALT: %v", err)
	}
	var sig haltSignal
	if err := json.Unmarshal(raw, &sig); err != nil {
		t.Fatalf("parse HALT: %v", err)
	}
	if sig.FailedVersion != "v7.5.0" {
		t.Errorf("FailedVersion = %q, want v7.5.0", sig.FailedVersion)
	}
	if sig.RollbackTo != "v7.1.0" {
		t.Errorf("RollbackTo = %q, want v7.1.0", sig.RollbackTo)
	}
	if sig.FailureMessage != "panic: simulated" {
		t.Errorf("FailureMessage = %q", sig.FailureMessage)
	}
	if sig.Timestamp == 0 {
		t.Error("Timestamp = 0, want unix-epoch second")
	}

	// Staging file must not survive
	if _, err := os.Stat(filepath.Join(dataDir, ".HALT.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf(".HALT.tmp leaked: %v", err)
	}
}

func TestWriteHaltSentinel_EmptyRollbackToIsValid(t *testing.T) {
	// The launcher resolves empty rollback_to via findLatestRollbackAnchor.
	// The writer must accept it without complaint.
	dataDir := t.TempDir()
	if err := writeHaltSentinel(dataDir, "v7.5.0", "", "post-upgrade migration failed"); err != nil {
		t.Fatalf("writeHaltSentinel with empty rollback: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dataDir, "HALT"))
	if err != nil {
		t.Fatalf("read HALT: %v", err)
	}
	var sig haltSignal
	if err := json.Unmarshal(raw, &sig); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if sig.RollbackTo != "" {
		t.Errorf("RollbackTo = %q, want empty", sig.RollbackTo)
	}
}

func TestWriteHaltSentinel_RejectsEmptyDataDir(t *testing.T) {
	if err := writeHaltSentinel("", "v7.5.0", "", "msg"); err == nil {
		t.Error("expected error for empty dataDir")
	}
}

func TestWriteHaltSentinel_RejectsEmptyFailedVersion(t *testing.T) {
	if err := writeHaltSentinel(t.TempDir(), "", "", "msg"); err == nil {
		t.Error("expected error for empty failedVersion")
	}
}

func TestHaltOnPanic_WritesSentinelAndRepanics(t *testing.T) {
	dataDir := t.TempDir()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic to propagate")
		}

		// Sentinel should be on disk
		raw, err := os.ReadFile(filepath.Join(dataDir, "HALT"))
		if err != nil {
			t.Fatalf("HALT missing after recover: %v", err)
		}
		var sig haltSignal
		if err := json.Unmarshal(raw, &sig); err != nil {
			t.Fatalf("parse HALT: %v", err)
		}
		// FailedVersion is whatever main.version is at test time ("dev")
		if sig.FailedVersion == "" {
			t.Errorf("FailedVersion empty; expected ldflag-injected value")
		}
		if sig.FailureMessage != "panic: simulated boom" {
			t.Errorf("FailureMessage = %q", sig.FailureMessage)
		}
	}()

	func() {
		defer haltOnPanic(dataDir)
		panic("simulated boom")
	}()
}
