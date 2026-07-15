package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/store"
)

// AppV20StateSyncBackupVerifier returns the exporter callback for a committed
// app-v20 height. It loads the backup into an isolated database, verifies the
// persisted height/fork, recomputes the narrow AppHash, and validates every
// canonical scoped envelope without mutating the live node.
func AppV20StateSyncBackupVerifier(height uint64) statesync.BackupVerifier {
	return func(ctx context.Context, backupPath string) ([]byte, error) {
		parent, err := os.MkdirTemp("", "sage-state-sync-verify-")
		if err != nil {
			return nil, err
		}
		defer func() { _ = os.RemoveAll(parent) }()
		return inspectAppV20StateSyncBackup(ctx, backupPath, filepath.Join(parent, "badger"), height)
	}
}

// PrepareAppV20StateSyncBackup loads and fully verifies a received backup into
// a fresh staging directory. It never touches the live Badger directory and
// removes targetDir on every failure. Successful atomic activation/rebinding is
// deliberately a separate release gate.
func PrepareAppV20StateSyncBackup(ctx context.Context, backupPath, targetDir string, height uint64, expectedAppHash []byte) error {
	if len(expectedAppHash) != sha256.Size {
		return errors.New("state sync expected AppHash must be SHA-256 sized")
	}
	computed, err := inspectAppV20StateSyncBackup(ctx, backupPath, targetDir, height)
	if err != nil {
		return err
	}
	if !bytes.Equal(computed, expectedAppHash) {
		_ = os.RemoveAll(targetDir)
		return errors.New("prepared state sync AppHash does not match trusted AppHash")
	}
	return nil
}

func inspectAppV20StateSyncBackup(ctx context.Context, backupPath, targetDir string, height uint64) ([]byte, error) {
	if backupPath == "" || targetDir == "" || height == 0 || height > math.MaxInt64 {
		return nil, errors.New("state sync backup path, target, and positive int64 height are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stat, err := os.Lstat(backupPath)
	if err != nil || !stat.Mode().IsRegular() || stat.Size() <= 0 || uint64(stat.Size()) > statesync.MaxSnapshotBytes { // #nosec G115 -- positive checked first
		return nil, errors.New("state sync backup is missing, non-regular, empty, or oversized")
	}
	if _, err := os.Stat(targetDir); err == nil {
		return nil, errors.New("state sync target already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(targetDir), 0o700); err != nil {
		return nil, err
	}
	if err := os.Mkdir(targetDir, 0o700); err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(targetDir)
		}
	}()

	db, err := badger.Open(badger.DefaultOptions(targetDir).WithLogger(nil))
	if err != nil {
		return nil, fmt.Errorf("open state sync staging badger: %w", err)
	}
	backup, err := os.Open(backupPath) //nolint:gosec // caller-selected regular backup file
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	loadErr := db.Load(backup, 16)
	closeBackupErr := backup.Close()
	closeDBErr := db.Close()
	if loadErr != nil {
		return nil, fmt.Errorf("load state sync Badger backup: %w", loadErr)
	}
	if closeBackupErr != nil {
		return nil, closeBackupErr
	}
	if closeDBErr != nil {
		return nil, closeDBErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	readOnly, err := store.OpenBadgerStoreReadOnly(targetDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = readOnly.CloseBadger() }()
	state, err := LoadState(readOnly)
	if err != nil {
		return nil, fmt.Errorf("load staged app state: %w", err)
	}
	if state.Height != int64(height) { // #nosec G115 -- bounded above by MaxInt64
		return nil, fmt.Errorf("staged app height %d does not match snapshot height %d", state.Height, height)
	}
	applied, err := readOnly.GetAppliedUpgrade(appV20UpgradeName)
	if err != nil {
		return nil, err
	}
	if applied == nil || applied.TargetAppVersion != 20 || applied.AppliedHeight <= 0 || int64(height) <= applied.AppliedHeight { // #nosec G115 -- bounded above by MaxInt64
		return nil, errors.New("state sync backup is not from an active post-app-v20 height")
	}
	computed, err := readOnly.ComputeAppHashExcludingBookkeeping()
	if err != nil {
		return nil, err
	}
	if len(state.AppHash) != sha256.Size || !bytes.Equal(state.AppHash, computed) {
		return nil, errors.New("staged persisted AppHash does not match staged Badger state")
	}
	probe := &SageApp{badgerStore: readOnly}
	if _, err := probe.VerifyScopedCanonicalState(); err != nil {
		return nil, fmt.Errorf("verify staged scoped state: %w", err)
	}
	keep = true
	return computed, nil
}
