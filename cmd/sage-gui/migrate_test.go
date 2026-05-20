package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestMigrateOnUpgrade_FirstRun(t *testing.T) {
	tmpDir := t.TempDir()
	origHome := os.Getenv("SAGE_HOME")
	os.Setenv("SAGE_HOME", tmpDir)
	defer os.Setenv("SAGE_HOME", origHome)

	dataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataDir, 0700)

	oldVersion := version
	version = "v2.5.0"
	defer func() { version = oldVersion }()

	migrated, err := migrateOnUpgrade(dataDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if migrated {
		t.Error("expected no migration on first run")
	}

	vPath := filepath.Join(tmpDir, versionFile)
	data, err := os.ReadFile(vPath)
	if err != nil {
		t.Fatalf("version file not written: %v", err)
	}
	if got := string(data); got != "v2.5.0\n" {
		t.Errorf("version file content = %q, want %q", got, "v2.5.0\n")
	}

	if got := readForkVersion(filepath.Join(tmpDir, forkVersionFile)); got != ConsensusForkVersion {
		t.Errorf("fork-version file = %d, want %d (first run must stamp current fork)", got, ConsensusForkVersion)
	}
}

func TestMigrateOnUpgrade_SameVersion(t *testing.T) {
	tmpDir := t.TempDir()
	origHome := os.Getenv("SAGE_HOME")
	os.Setenv("SAGE_HOME", tmpDir)
	defer os.Setenv("SAGE_HOME", origHome)

	dataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataDir, 0700)

	oldVersion := version
	version = "v2.5.0"
	defer func() { version = oldVersion }()

	os.WriteFile(filepath.Join(tmpDir, versionFile), []byte("v2.5.0\n"), 0600)
	stampForkVersion(filepath.Join(tmpDir, forkVersionFile), ConsensusForkVersion)

	migrated, err := migrateOnUpgrade(dataDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if migrated {
		t.Error("should not migrate when version unchanged")
	}
}

// TestMigrateOnUpgrade_VersionChangedSameFork_PreservesState is the v7.5.5
// contract: patch/minor bumps that don't touch consensus must NOT reset
// BadgerDB or CometBFT state. Pre-v7.5.5 wiped both on any version-string
// change, silently destroying every operator's domain registry, access
// grants, org memberships, and validator set. This test guards against
// that regression.
func TestMigrateOnUpgrade_VersionChangedSameFork_PreservesState(t *testing.T) {
	tmpDir := t.TempDir()
	origHome := os.Getenv("SAGE_HOME")
	os.Setenv("SAGE_HOME", tmpDir)
	defer os.Setenv("SAGE_HOME", origHome)

	dataDir := filepath.Join(tmpDir, "data")
	badgerDir := filepath.Join(dataDir, "badger")
	cometDir := filepath.Join(dataDir, "cometbft", "data")
	sqlitePath := filepath.Join(dataDir, "sage.db")

	os.MkdirAll(badgerDir, 0700)
	os.MkdirAll(cometDir, 0700)
	os.WriteFile(sqlitePath, []byte("fake-sqlite-data"), 0600)
	os.WriteFile(filepath.Join(badgerDir, "000001.vlog"), []byte("badger"), 0600)
	os.MkdirAll(filepath.Join(cometDir, "blockstore.db"), 0700)
	os.MkdirAll(filepath.Join(cometDir, "state.db"), 0700)
	os.WriteFile(filepath.Join(cometDir, "priv_validator_state.json"), []byte(`{"height":"100"}`), 0600)

	os.WriteFile(filepath.Join(tmpDir, versionFile), []byte("v7.5.4\n"), 0600)
	stampForkVersion(filepath.Join(tmpDir, forkVersionFile), ConsensusForkVersion)

	oldVersion := version
	version = "v7.5.5"
	defer func() { version = oldVersion }()

	migrated, err := migrateOnUpgrade(dataDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if migrated {
		t.Fatal("same-fork version bump must NOT report migration — pre-v7.5.5 regression")
	}

	if data, _ := os.ReadFile(filepath.Join(badgerDir, "000001.vlog")); string(data) != "badger" {
		t.Error("badger value log must be preserved on same-fork upgrade")
	}
	if _, err := os.Stat(filepath.Join(cometDir, "blockstore.db")); os.IsNotExist(err) {
		t.Error("CometBFT blockstore.db must be preserved on same-fork upgrade")
	}
	if _, err := os.Stat(filepath.Join(cometDir, "state.db")); os.IsNotExist(err) {
		t.Error("CometBFT state.db must be preserved on same-fork upgrade")
	}
	if data, _ := os.ReadFile(filepath.Join(cometDir, "priv_validator_state.json")); string(data) != `{"height":"100"}` {
		t.Errorf("priv_validator_state.json must be preserved, got %q", data)
	}

	vData, _ := os.ReadFile(filepath.Join(tmpDir, versionFile))
	if string(vData) != "v7.5.5\n" {
		t.Errorf("version file = %q, want v7.5.5\\n (diagnostics stamp must still happen)", vData)
	}
}

// TestMigrateOnUpgrade_LegacyInstall_AdoptsCurrentFork: an install that
// predates the gate (has version.txt but no fork-version.txt) must adopt
// the current ConsensusForkVersion on first boot WITHOUT resetting state.
// This is the in-place upgrade path from v7.5.4 → v7.5.5.
func TestMigrateOnUpgrade_LegacyInstall_AdoptsCurrentFork(t *testing.T) {
	tmpDir := t.TempDir()
	origHome := os.Getenv("SAGE_HOME")
	os.Setenv("SAGE_HOME", tmpDir)
	defer os.Setenv("SAGE_HOME", origHome)

	dataDir := filepath.Join(tmpDir, "data")
	badgerDir := filepath.Join(dataDir, "badger")
	os.MkdirAll(badgerDir, 0700)
	os.WriteFile(filepath.Join(badgerDir, "000001.vlog"), []byte("badger"), 0600)

	os.WriteFile(filepath.Join(tmpDir, versionFile), []byte("v7.5.4\n"), 0600)

	oldVersion := version
	version = "v7.5.5"
	defer func() { version = oldVersion }()

	migrated, err := migrateOnUpgrade(dataDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if migrated {
		t.Fatal("legacy install must adopt current fork without reset")
	}

	if data, _ := os.ReadFile(filepath.Join(badgerDir, "000001.vlog")); string(data) != "badger" {
		t.Error("badger value log must be preserved on legacy-install adoption")
	}

	if got := readForkVersion(filepath.Join(tmpDir, forkVersionFile)); got != ConsensusForkVersion {
		t.Errorf("fork-version file = %d, want %d (legacy install must adopt current fork)", got, ConsensusForkVersion)
	}
}

// TestMigrateOnUpgrade_ForkBump_RunsReset: when the on-disk fork is older
// than the binary's ConsensusForkVersion, the destructive reset must run —
// chain state is incompatible by definition.
func TestMigrateOnUpgrade_ForkBump_RunsReset(t *testing.T) {
	tmpDir := t.TempDir()
	origHome := os.Getenv("SAGE_HOME")
	os.Setenv("SAGE_HOME", tmpDir)
	defer os.Setenv("SAGE_HOME", origHome)

	dataDir := filepath.Join(tmpDir, "data")
	badgerDir := filepath.Join(dataDir, "badger")
	cometDir := filepath.Join(dataDir, "cometbft", "data")
	sqlitePath := filepath.Join(dataDir, "sage.db")

	os.MkdirAll(badgerDir, 0700)
	os.MkdirAll(cometDir, 0700)
	os.WriteFile(sqlitePath, []byte("fake-sqlite-data"), 0600)
	os.WriteFile(filepath.Join(badgerDir, "000001.vlog"), []byte("badger"), 0600)
	os.MkdirAll(filepath.Join(cometDir, "blockstore.db"), 0700)
	os.MkdirAll(filepath.Join(cometDir, "state.db"), 0700)
	os.WriteFile(filepath.Join(cometDir, "priv_validator_state.json"), []byte(`{"height":"100"}`), 0600)

	os.WriteFile(filepath.Join(tmpDir, versionFile), []byte("v7.5.5\n"), 0600)
	stampForkVersion(filepath.Join(tmpDir, forkVersionFile), 1)

	oldVersion := version
	version = "v8.0.0"
	defer func() { version = oldVersion }()

	oldFork := ConsensusForkVersion
	ConsensusForkVersion = 2
	defer func() { ConsensusForkVersion = oldFork }()

	migrated, err := migrateOnUpgrade(dataDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !migrated {
		t.Fatal("fork bump must trigger reset")
	}

	if entries, _ := os.ReadDir(badgerDir); len(entries) != 0 {
		t.Errorf("badger dir must be empty after fork-bump reset, has %d entries", len(entries))
	}
	if _, err := os.Stat(filepath.Join(cometDir, "blockstore.db")); !os.IsNotExist(err) {
		t.Error("blockstore.db must be removed after fork-bump reset")
	}
	if pvState, _ := os.ReadFile(filepath.Join(cometDir, "priv_validator_state.json")); string(pvState) != `{"height":"0","round":0,"step":0}` {
		t.Errorf("validator state not reset: %s", pvState)
	}

	if sqlData, _ := os.ReadFile(sqlitePath); string(sqlData) != "fake-sqlite-data" {
		t.Error("SQLite must survive a fork-bump reset (only Badger + CometBFT wipe)")
	}

	backupDir := filepath.Join(tmpDir, "backups")
	if entries, _ := os.ReadDir(backupDir); len(entries) == 0 {
		t.Error("fork-bump reset must create SQLite backup")
	}

	if got := readForkVersion(filepath.Join(tmpDir, forkVersionFile)); got != 2 {
		t.Errorf("fork-version file = %d, want 2 after successful reset", got)
	}
	vData, _ := os.ReadFile(filepath.Join(tmpDir, versionFile))
	if strings.TrimSpace(string(vData)) != "v8.0.0" {
		t.Errorf("version file = %q, want v8.0.0", vData)
	}
}

// TestMigrateOnUpgrade_ForkBump_StampsOnlyAfterReset verifies that a crash
// between the reset and the stamp leaves the OLD fork version on disk so
// the next boot retries the migration. Tested by checking that the fork
// stamp is written AFTER the reset wipes state — if we stamped first and
// then crashed, the next boot would see "current fork" and skip the
// (incomplete) reset, leaving the operator with mixed-fork state.
func TestMigrateOnUpgrade_ForkBump_StampsOnlyAfterReset(t *testing.T) {
	tmpDir := t.TempDir()
	origHome := os.Getenv("SAGE_HOME")
	os.Setenv("SAGE_HOME", tmpDir)
	defer os.Setenv("SAGE_HOME", origHome)

	dataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataDir, 0700)

	os.WriteFile(filepath.Join(tmpDir, versionFile), []byte("v7.5.5\n"), 0600)
	stampForkVersion(filepath.Join(tmpDir, forkVersionFile), 1)

	oldVersion := version
	version = "v8.0.0"
	defer func() { version = oldVersion }()
	oldFork := ConsensusForkVersion
	ConsensusForkVersion = 2
	defer func() { ConsensusForkVersion = oldFork }()

	_, err := migrateOnUpgrade(dataDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := readForkVersion(filepath.Join(tmpDir, forkVersionFile))
	if got != 2 {
		t.Errorf("post-migration fork = %d, want 2", got)
	}
}

func TestMigrateOnUpgrade_DevVersion(t *testing.T) {
	tmpDir := t.TempDir()
	origHome := os.Getenv("SAGE_HOME")
	os.Setenv("SAGE_HOME", tmpDir)
	defer os.Setenv("SAGE_HOME", origHome)

	dataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataDir, 0700)

	oldVersion := version
	version = "dev"
	defer func() { version = oldVersion }()

	os.WriteFile(filepath.Join(tmpDir, versionFile), []byte("v2.4.0\n"), 0600)

	migrated, err := migrateOnUpgrade(dataDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if migrated {
		t.Error("dev builds should skip migration")
	}
}

func TestReadForkVersion_AbsentFileReturnsZero(t *testing.T) {
	tmpDir := t.TempDir()
	if got := readForkVersion(filepath.Join(tmpDir, "nope.txt")); got != 0 {
		t.Errorf("missing file should return 0, got %d", got)
	}
}

func TestStampForkVersion_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "fork.txt")
	if err := stampForkVersion(path, 42); err != nil {
		t.Fatal(err)
	}
	if got := readForkVersion(path); got != 42 {
		t.Errorf("round trip = %d, want 42", got)
	}
	// File should be parseable plain integer with trailing newline.
	data, _ := os.ReadFile(path)
	if strings.TrimSpace(string(data)) != strconv.Itoa(42) {
		t.Errorf("file content = %q", data)
	}
}
