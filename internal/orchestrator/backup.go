package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// BackupSQLite creates an atomic backup of the SQLite database using VACUUM INTO.
// This is safe even during concurrent WAL writes.
func BackupSQLite(dataDir string) error {
	dbPath := filepath.Join(dataDir, "sage.db")

	// Ensure backup directory exists
	backupDir := filepath.Join(dataDir, "..", "backups")
	if err := os.MkdirAll(backupDir, 0700); err != nil {
		return fmt.Errorf("create backup dir: %w", err)
	}

	ts := time.Now().Format("2006-01-02T15-04-05")
	backupPath := filepath.Join(backupDir, fmt.Sprintf("sage-pre-redeploy-%s.db", ts))

	// Open source DB for VACUUM INTO
	dsn := dbPath + "?_journal_mode=WAL&_busy_timeout=15000&mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open db for backup: %w", err)
	}
	defer func() { _ = db.Close() }()

	// VACUUM INTO creates an atomic consistent snapshot
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, fmt.Sprintf(`VACUUM INTO '%s'`, backupPath))
	if err != nil {
		return fmt.Errorf("VACUUM INTO backup: %w", err)
	}

	// Also backup genesis.json
	genesisPath := filepath.Join(dataDir, "cometbft", "config", "genesis.json")
	if data, readErr := os.ReadFile(genesisPath); readErr == nil {
		genesisBackup := filepath.Join(backupDir, fmt.Sprintf("genesis-pre-redeploy-%s.json", ts))
		if writeErr := os.WriteFile(genesisBackup, data, 0600); writeErr != nil { //nolint:gosec // path is constructed from trusted dataDir
			return fmt.Errorf("backup genesis: %w", writeErr)
		}
	}

	return nil
}

// RestoreSQLiteBackup restores the most recent backup.
func RestoreSQLiteBackup(dataDir string) error {
	backupDir := filepath.Join(dataDir, "..", "backups")
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return fmt.Errorf("read backup dir: %w", err)
	}

	// Find most recent backup
	var latest string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".db" {
			if e.Name() > latest {
				latest = e.Name()
			}
		}
	}
	if latest == "" {
		return fmt.Errorf("no backup found")
	}

	backupPath := filepath.Join(backupDir, latest)
	dbPath := filepath.Join(dataDir, "sage.db")

	data, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("read backup: %w", err)
	}

	if err := os.WriteFile(dbPath, data, 0600); err != nil { //nolint:gosec // path is constructed from trusted dataDir
		return fmt.Errorf("restore backup: %w", err)
	}

	return nil
}

// BackupGenesis copies the current genesis.json to the backup directory.
func BackupGenesis(dataDir string) (string, error) {
	genesisPath := filepath.Join(dataDir, "cometbft", "config", "genesis.json")
	data, err := os.ReadFile(genesisPath)
	if err != nil {
		return "", fmt.Errorf("read genesis: %w", err)
	}

	backupDir := filepath.Join(dataDir, "..", "backups")
	if mkErr := os.MkdirAll(backupDir, 0700); mkErr != nil {
		return "", fmt.Errorf("create backup dir: %w", mkErr)
	}

	ts := time.Now().Format("2006-01-02T15-04-05")
	backupPath := filepath.Join(backupDir, fmt.Sprintf("genesis-%s.json", ts))
	if err := os.WriteFile(backupPath, data, 0600); err != nil { //nolint:gosec // path is constructed from trusted dataDir
		return "", fmt.Errorf("write genesis backup: %w", err)
	}

	return backupPath, nil
}
