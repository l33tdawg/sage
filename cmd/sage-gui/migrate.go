package main

import (
	"context"
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

	// Step 0: Protect the vault key — back it up before touching anything.
	// The vault key is irreplaceable: if lost, all encrypted memories become
	// permanently unrecoverable. We back it up on every upgrade as a safety net.
	vaultKeyPath := filepath.Join(SageHome(), "vault.key")
	if _, vkErr := os.Stat(vaultKeyPath); vkErr == nil {
		backupDir := filepath.Join(SageHome(), "backups")
		_ = os.MkdirAll(backupDir, 0700)
		ts := time.Now().Format("2006-01-02T15-04-05")
		vaultBackup := filepath.Join(backupDir, fmt.Sprintf("vault-pre-upgrade-%s-%s.key", lastVersion, ts))
		if src, readErr := os.ReadFile(vaultKeyPath); readErr == nil {
			if writeErr := os.WriteFile(vaultBackup, src, 0600); writeErr == nil { //nolint:gosec // trusted local vault backup
				fmt.Fprintf(os.Stderr, "  Backed up vault key to %s\n", vaultBackup)
			}
		}
	}

	// Step 1: Backup SQLite (the precious data) using VACUUM INTO for atomic consistency
	if _, statErr := os.Stat(sqlitePath); statErr == nil {
		// Clean up stale WAL/SHM files from previous version — these can cause
		// hangs when the new binary tries to open the database.
		for _, suffix := range []string{"-wal", "-shm"} {
			walPath := sqlitePath + suffix
			if _, walErr := os.Stat(walPath); walErr == nil {
				// Checkpoint WAL into main DB first, then remove
				if cpErr := checkpointWAL(sqlitePath); cpErr != nil {
					fmt.Fprintf(os.Stderr, "  Warning: WAL checkpoint failed: %v (removing stale WAL)\n", cpErr)
				}
				_ = os.Remove(walPath)
			}
		}

		backupDir := filepath.Join(SageHome(), "backups")
		if mkErr := os.MkdirAll(backupDir, 0700); mkErr != nil {
			return false, fmt.Errorf("create backup dir: %w", mkErr)
		}
		ts := time.Now().Format("2006-01-02T15-04-05")
		backupPath := filepath.Join(backupDir, fmt.Sprintf("sage-pre-upgrade-%s-%s.db", lastVersion, ts))

		// Use VACUUM INTO with a 30-second timeout to prevent indefinite hangs.
		vacuumErr := vacuumBackup(sqlitePath, backupPath)
		if vacuumErr != nil {
			// Fallback to file copy if VACUUM INTO fails or times out
			fmt.Fprintf(os.Stderr, "  VACUUM INTO failed (%v), falling back to file copy\n", vacuumErr)
			src, readErr := os.ReadFile(sqlitePath)
			if readErr != nil {
				return false, fmt.Errorf("read sqlite for backup: %w", readErr)
			}
			if writeErr := os.WriteFile(backupPath, src, 0600); writeErr != nil { //nolint:gosec // backupPath is server-controlled
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

	// Step 4: Clean up low-quality and duplicate memories in SQLite
	// v4.0.0 introduces memory quality validators — clean up existing noise
	if _, statErr := os.Stat(sqlitePath); statErr == nil {
		cleaned := cleanupNoisyMemories(sqlitePath)
		if cleaned > 0 {
			fmt.Fprintf(os.Stderr, "  Cleaned up %d low-quality/duplicate memories\n", cleaned)
		}
	}

	// Step 5: Stamp new version
	if stampErr := stampVersion(versionPath); stampErr != nil {
		return false, stampErr
	}

	fmt.Fprintf(os.Stderr, "  Upgrade complete — your memories are safe, chain will rebuild\n\n")
	return true, nil
}

// cleanupNoisyMemories deprecates duplicate boot safeguards, noise observations,
// and empty reflections that accumulated before v4.0.0's quality validators.
// Returns the number of memories deprecated.
func cleanupNoisyMemories(sqlitePath string) int {
	dsn := sqlitePath + "?_journal_mode=WAL&_busy_timeout=15000"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return 0
	}
	defer func() { _ = db.Close() }()

	// Use a 60-second timeout for the entire cleanup operation
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	deprecated := 0

	// 1. Deduplicate boot safeguard memories — keep only the newest one per agent
	rows, err := db.QueryContext(ctx, `
		SELECT memory_id FROM memories
		WHERE domain_tag = 'meta'
		  AND status = 'committed'
		  AND (content LIKE '%sage_inception%' OR content LIKE '%boot sequence%' OR content LIKE '%BOOT SAFEGUARD%')
		ORDER BY created_at DESC`)
	if err == nil {
		var ids []string
		for rows.Next() {
			var id string
			if scanErr := rows.Scan(&id); scanErr == nil {
				ids = append(ids, id)
			}
		}
		_ = rows.Close()
		// Keep the first (newest), deprecate the rest
		if len(ids) > 1 {
			for _, id := range ids[1:] {
				if _, execErr := db.ExecContext(ctx, `UPDATE memories SET status = 'deprecated' WHERE memory_id = ?`, id); execErr == nil {
					deprecated++
				}
			}
		}
	}

	// 2. Deprecate noise observations (short/low-value content)
	noisePatterns := []string{
		"%user said hi%", "%user greeted%", "%session started%",
		"%brain online%", "%brain is awake%", "%no action taken%",
		"%user said morning%", "%new session started%",
		"%user said hello%", "%greeted the user%",
	}
	for _, pattern := range noisePatterns {
		res, execErr := db.ExecContext(ctx, `UPDATE memories SET status = 'deprecated'
			WHERE status = 'committed' AND LOWER(content) LIKE ?`, pattern)
		if execErr == nil {
			if n, _ := res.RowsAffected(); n > 0 {
				deprecated += int(n)
			}
		}
	}

	// 3. Deprecate very short observations (< 20 chars content)
	res, err := db.ExecContext(ctx, `UPDATE memories SET status = 'deprecated'
		WHERE status = 'committed' AND memory_type = 'observation' AND LENGTH(content) < 20`)
	if err == nil {
		if n, _ := res.RowsAffected(); n > 0 {
			deprecated += int(n)
		}
	}

	// 4. Deduplicate — deprecate memories with identical content_hash, keep newest
	dupRows, err := db.QueryContext(ctx, `
		SELECT content_hash FROM memories
		WHERE status = 'committed' AND content_hash IS NOT NULL
		GROUP BY content_hash HAVING COUNT(*) > 1`)
	if err == nil {
		var hashes [][]byte
		for dupRows.Next() {
			var h []byte
			if scanErr := dupRows.Scan(&h); scanErr == nil {
				hashes = append(hashes, h)
			}
		}
		_ = dupRows.Close()
		for _, h := range hashes {
			// Keep the newest, deprecate the rest
			res, execErr := db.ExecContext(ctx, `UPDATE memories SET status = 'deprecated'
				WHERE content_hash = ? AND status = 'committed'
				AND memory_id NOT IN (
					SELECT memory_id FROM memories
					WHERE content_hash = ? AND status = 'committed'
					ORDER BY created_at DESC LIMIT 1
				)`, h, h)
			if execErr == nil {
				if n, _ := res.RowsAffected(); n > 0 {
					deprecated += int(n)
				}
			}
		}
	}

	return deprecated
}

// checkpointWAL forces a WAL checkpoint on the database, merging any
// pending WAL writes into the main DB file. This prevents stale WAL files
// from causing hangs on upgrade.
func checkpointWAL(dbPath string) error {
	dsn := dbPath + "?_busy_timeout=5000"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

// vacuumBackup creates an atomic backup using VACUUM INTO with a timeout.
func vacuumBackup(srcPath, dstPath string) error {
	dsn := srcPath + "?_journal_mode=WAL&_busy_timeout=15000"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, fmt.Sprintf(`VACUUM INTO '%s'`, dstPath))
	return err
}

func stampVersion(path string) error {
	return os.WriteFile(path, []byte(version+"\n"), 0600)
}
