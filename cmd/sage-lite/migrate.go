package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver
)

const versionFile = "version.txt"

// migrateOnUpgrade checks if the binary version has changed since last run.
// If so, it backs up the SQLite database and resets chain state (BadgerDB +
// CometBFT) so the fresh chain can re-validate existing memories.
// This ensures drag-and-drop upgrades just work for end users.
func migrateOnUpgrade(dataDir string) (migrated bool, err error) {
	versionPath := filepath.Join(SageHome(), versionFile)
	cometHome := filepath.Join(dataDir, "cometbft")
	badgerPath := filepath.Join(dataDir, "badger")
	sqlitePath := filepath.Join(dataDir, "sage.db")

	// Read last-run version
	lastVersion := ""
	if data, readErr := os.ReadFile(versionPath); readErr == nil {
		lastVersion = strings.TrimSpace(string(data))
	}

	// If same version (or "dev" builds), skip migration
	if lastVersion == version || version == "dev" {
		return false, nil
	}

	// First run ever — no migration needed, just stamp version
	if lastVersion == "" {
		return false, stampVersion(versionPath)
	}

	// Version changed — need migration
	fmt.Fprintf(os.Stderr, "\n  Upgrading SAGE from %s → %s\n", lastVersion, version)

	// Step 1: Backup SQLite (the precious data) using VACUUM INTO for atomic consistency
	if _, statErr := os.Stat(sqlitePath); statErr == nil {
		backupDir := filepath.Join(SageHome(), "backups")
		if mkErr := os.MkdirAll(backupDir, 0700); mkErr != nil {
			return false, fmt.Errorf("create backup dir: %w", mkErr)
		}
		ts := time.Now().Format("2006-01-02T15-04-05")
		backupPath := filepath.Join(backupDir, fmt.Sprintf("sage-pre-upgrade-%s-%s.db", lastVersion, ts))

		// Use VACUUM INTO for atomic, consistent snapshot (safe during concurrent WAL writes)
		dsn := sqlitePath + "?_journal_mode=WAL&_busy_timeout=5000"
		srcDB, openErr := sql.Open("sqlite", dsn)
		if openErr != nil {
			return false, fmt.Errorf("open sqlite for backup: %w", openErr)
		}
		_, vacuumErr := srcDB.Exec(fmt.Sprintf(`VACUUM INTO '%s'`, backupPath))
		srcDB.Close()
		if vacuumErr != nil {
			// Fallback to file copy if VACUUM INTO fails (e.g., older SQLite)
			src, readErr := os.ReadFile(sqlitePath)
			if readErr != nil {
				return false, fmt.Errorf("read sqlite for backup: %w", readErr)
			}
			if writeErr := os.WriteFile(backupPath, src, 0600); writeErr != nil {
				return false, fmt.Errorf("write backup: %w", writeErr)
			}
		}
		fmt.Fprintf(os.Stderr, "  Backed up memories to %s\n", backupPath)
	}

	// Step 2: Wipe BadgerDB (on-chain state — will be rebuilt)
	if _, statErr := os.Stat(badgerPath); statErr == nil {
		if removeErr := os.RemoveAll(badgerPath); removeErr != nil {
			return false, fmt.Errorf("remove badger: %w", removeErr)
		}
		if mkErr := os.MkdirAll(badgerPath, 0700); mkErr != nil {
			return false, fmt.Errorf("recreate badger dir: %w", mkErr)
		}
		fmt.Fprintf(os.Stderr, "  Reset on-chain state (BadgerDB)\n")
	}

	// Step 3: Wipe CometBFT data (blocks/state — incompatible with new chain)
	// Keep config (genesis, keys) but remove block/state databases
	cometDataDir := filepath.Join(cometHome, "data")
	if _, statErr := os.Stat(cometDataDir); statErr == nil {
		// Remove the block/state DBs but keep priv_validator_state.json
		for _, dbName := range []string{"blockstore.db", "state.db", "tx_index.db", "evidence.db"} {
			dbPath := filepath.Join(cometDataDir, dbName)
			if removeErr := os.RemoveAll(dbPath); removeErr != nil {
				fmt.Fprintf(os.Stderr, "  Warning: could not remove %s: %v\n", dbName, removeErr)
			}
		}
		// Reset validator state to height 0
		pvStatePath := filepath.Join(cometDataDir, "priv_validator_state.json")
		pvState := []byte(`{"height":"0","round":0,"step":0}`)
		if writeErr := os.WriteFile(pvStatePath, pvState, 0600); writeErr != nil {
			fmt.Fprintf(os.Stderr, "  Warning: could not reset validator state: %v\n", writeErr)
		}
		fmt.Fprintf(os.Stderr, "  Reset blockchain state (CometBFT)\n")
	}

	// Step 4: Stamp new version
	if stampErr := stampVersion(versionPath); stampErr != nil {
		return false, stampErr
	}

	fmt.Fprintf(os.Stderr, "  Upgrade complete — your memories are safe, chain will rebuild\n\n")
	return true, nil
}

func stampVersion(path string) error {
	return os.WriteFile(path, []byte(version+"\n"), 0600)
}
