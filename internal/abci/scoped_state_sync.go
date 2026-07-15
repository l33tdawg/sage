package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/poe"
	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/store"
)

// AppV20StateSyncBackupVerifier returns the exporter callback for a committed
// app-v20 height. It restores the canonical image into an isolated database, verifies the
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

// PrepareAppV20StateSyncBackup restores and fully verifies a received canonical image into
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

// InspectAppV20StateSyncDirectory opens an existing closed Badger directory
// read-only and returns its verified persisted height and narrow AppHash. Boot
// recovery uses this before any live store is opened or ABCI handshake begins.
func InspectAppV20StateSyncDirectory(ctx context.Context, path string) (uint64, []byte, error) {
	if path == "" {
		return 0, nil, errors.New("state sync directory path is required")
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return 0, nil, contextErr
	}
	readOnly, err := store.OpenBadgerStoreReadOnly(path)
	if err != nil {
		return 0, nil, err
	}
	height, appHash, inspectErr := inspectAppV20StateSyncStore(ctx, readOnly, "state sync")
	closeErr := readOnly.CloseBadger()
	if inspectErr != nil {
		return 0, nil, inspectErr
	}
	if closeErr != nil {
		return 0, nil, closeErr
	}
	return height, appHash, nil
}

// InspectStateSyncRecoveryDirectory accepts either a canonical fresh pre-chain
// store (height 0, empty AppHash) or a fully verified positive-height app-v20
// store. Fresh joining nodes have no trusted application hash yet, but their
// quarantined pre-activation directory must still be recoverable after a crash.
func InspectStateSyncRecoveryDirectory(ctx context.Context, path string) (uint64, []byte, error) {
	if path == "" {
		return 0, nil, errors.New("state sync recovery directory path is required")
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return 0, nil, contextErr
	}
	readOnly, err := store.OpenBadgerStoreReadOnly(path)
	if err != nil {
		return 0, nil, err
	}
	state, stateErr := LoadState(readOnly)
	if stateErr != nil {
		_ = readOnly.CloseBadger()
		return 0, nil, fmt.Errorf("load state sync recovery app state: %w", stateErr)
	}
	if state.Height == 0 {
		heightBytes, heightErr := readOnly.GetState(stateHeightKey)
		if heightErr != nil || (heightBytes != nil && (len(heightBytes) != 8 || binary.BigEndian.Uint64(heightBytes) != 0)) {
			_ = readOnly.CloseBadger()
			return 0, nil, errors.New("fresh state sync recovery directory has invalid height bookkeeping")
		}
		storedAppHash, appHashErr := readOnly.GetState(stateAppHashKey)
		if appHashErr != nil || len(storedAppHash) != 0 {
			_ = readOnly.CloseBadger()
			return 0, nil, errors.New("fresh state sync recovery directory has a non-empty AppHash")
		}
		epochBytes, epochErr := readOnly.GetState(stateEpochKey)
		if epochErr != nil || (epochBytes != nil && (len(epochBytes) != 8 || binary.BigEndian.Uint64(epochBytes) != 0)) {
			_ = readOnly.CloseBadger()
			return 0, nil, errors.New("fresh state sync recovery directory has invalid epoch bookkeeping")
		}
		if len(state.AppHash) != 0 {
			_ = readOnly.CloseBadger()
			return 0, nil, errors.New("fresh state sync recovery directory has a non-empty AppHash")
		}
		computed, computeErr := readOnly.ComputeAppHashExcludingBookkeeping()
		if computeErr != nil {
			_ = readOnly.CloseBadger()
			return 0, nil, computeErr
		}
		emptyHash := sha256.Sum256(nil)
		if !bytes.Equal(computed, emptyHash[:]) {
			_ = readOnly.CloseBadger()
			return 0, nil, errors.New("fresh state sync recovery directory contains consensus state")
		}
		closeErr := readOnly.CloseBadger()
		if closeErr != nil {
			return 0, nil, closeErr
		}
		return 0, nil, nil
	}
	if state.Height < 0 {
		_ = readOnly.CloseBadger()
		return 0, nil, errors.New("state sync recovery directory has a negative height")
	}
	if len(state.AppHash) != sha256.Size {
		_ = readOnly.CloseBadger()
		return 0, nil, errors.New("positive-height state sync recovery directory has an invalid AppHash")
	}
	appV20, upgradeErr := readOnly.GetAppliedUpgrade(appV20UpgradeName)
	if upgradeErr != nil {
		_ = readOnly.CloseBadger()
		return 0, nil, upgradeErr
	}
	// A quarantine may legitimately be from any older supported app version.
	// Pre-app-v12 AppHashes include bookkeeping that SaveState changes after
	// FinalizeBlock, so they cannot be recomputed from the closed post-Commit DB.
	// The persisted height/AppHash are still compared exactly with Comet by the
	// recovery executor. Once app-v20 is active, apply its stronger narrow-hash
	// and scoped-state checks as well.
	if appV20 == nil || appV20.TargetAppVersion != 20 || appV20.AppliedHeight <= 0 || state.Height <= appV20.AppliedHeight {
		closeErr := readOnly.CloseBadger()
		if closeErr != nil {
			return 0, nil, closeErr
		}
		return uint64(state.Height), append([]byte(nil), state.AppHash...), nil // #nosec G115 -- positive int64 checked above
	}
	height, appHash, inspectErr := inspectAppV20StateSyncStore(ctx, readOnly, "state sync recovery")
	closeErr := readOnly.CloseBadger()
	if inspectErr != nil {
		return 0, nil, inspectErr
	}
	if closeErr != nil {
		return 0, nil, closeErr
	}
	return height, appHash, nil
}

// VerifyActivatedAppV20StateSyncDirectory reopens the promoted directory with
// the writable no-migration constructor, re-verifies its exact trusted state,
// and closes it. The runtime constructs the replacement SageApp only after this
// succeeds.
func VerifyActivatedAppV20StateSyncDirectory(ctx context.Context, path string, expectedHeight uint64, expectedAppHash []byte) error {
	if path == "" || expectedHeight == 0 || expectedHeight > math.MaxInt64 || len(expectedAppHash) != sha256.Size {
		return errors.New("activated state sync path, positive int64 height, and SHA-256 AppHash are required")
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	writable, err := store.OpenBadgerStoreWithoutMigrations(path)
	if err != nil {
		return err
	}
	height, appHash, inspectErr := inspectAppV20StateSyncStore(ctx, writable, "activated")
	closeErr := writable.CloseBadger()
	if inspectErr != nil {
		return inspectErr
	}
	if closeErr != nil {
		return closeErr
	}
	if height != expectedHeight || !bytes.Equal(appHash, expectedAppHash) {
		return errors.New("activated state sync directory does not match trusted state")
	}
	return nil
}

func inspectAppV20StateSyncBackup(ctx context.Context, backupPath, targetDir string, height uint64) ([]byte, error) {
	if backupPath == "" || targetDir == "" || height == 0 || height > math.MaxInt64 {
		return nil, errors.New("state sync backup path, target, and positive int64 height are required")
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	stat, statErr := os.Lstat(backupPath)
	if statErr != nil || !stat.Mode().IsRegular() || stat.Size() <= 0 || uint64(stat.Size()) > statesync.MaxSnapshotBytes { // #nosec G115 -- positive checked first
		return nil, errors.New("state sync backup is missing, non-regular, empty, or oversized")
	}
	if _, targetErr := os.Stat(targetDir); targetErr == nil {
		return nil, errors.New("state sync target already exists")
	} else if !errors.Is(targetErr, os.ErrNotExist) {
		return nil, targetErr
	}
	if mkdirParentErr := os.MkdirAll(filepath.Dir(targetDir), 0o700); mkdirParentErr != nil {
		return nil, mkdirParentErr
	}
	if mkdirTargetErr := os.Mkdir(targetDir, 0o700); mkdirTargetErr != nil {
		return nil, mkdirTargetErr
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
	loadErr := statesync.RestoreCanonicalState(ctx, backup, db)
	closeBackupErr := backup.Close()
	closeDBErr := db.Close()
	if loadErr != nil {
		return nil, fmt.Errorf("restore state sync canonical state: %w", loadErr)
	}
	if closeBackupErr != nil {
		return nil, closeBackupErr
	}
	if closeDBErr != nil {
		return nil, closeDBErr
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}

	readOnly, err := store.OpenBadgerStoreReadOnly(targetDir)
	if err != nil {
		return nil, err
	}
	actualHeight, computed, inspectErr := inspectAppV20StateSyncStore(ctx, readOnly, "staged")
	closeReadOnlyErr := readOnly.CloseBadger()
	if inspectErr != nil {
		return nil, inspectErr
	}
	if closeReadOnlyErr != nil {
		return nil, closeReadOnlyErr
	}
	if actualHeight != height {
		return nil, fmt.Errorf("staged app height %d does not match snapshot height %d", actualHeight, height)
	}
	keep = true
	return computed, nil
}

func inspectAppV20StateSyncStore(ctx context.Context, badgerStore *store.BadgerStore, label string) (uint64, []byte, error) {
	if contextErr := ctx.Err(); contextErr != nil {
		return 0, nil, contextErr
	}
	state, err := LoadState(badgerStore)
	if err != nil {
		return 0, nil, fmt.Errorf("load %s app state: %w", label, err)
	}
	if state.Height <= 0 {
		return 0, nil, fmt.Errorf("%s app height must be positive", label)
	}
	if state.EpochNum != poe.EpochNumber(state.Height) {
		return 0, nil, fmt.Errorf("%s persisted epoch does not match height", label)
	}
	applied, err := badgerStore.GetAppliedUpgrade(appV20UpgradeName)
	if err != nil {
		return 0, nil, err
	}
	if applied == nil || applied.TargetAppVersion != 20 || applied.AppliedHeight <= 0 || state.Height <= applied.AppliedHeight {
		return 0, nil, errors.New("state sync state is not from an active post-app-v20 height")
	}
	computed, err := badgerStore.ComputeAppHashExcludingBookkeeping()
	if err != nil {
		return 0, nil, err
	}
	if len(state.AppHash) != sha256.Size || !bytes.Equal(state.AppHash, computed) {
		return 0, nil, fmt.Errorf("%s persisted AppHash does not match Badger state", label)
	}
	probe := &SageApp{badgerStore: badgerStore}
	if _, err := probe.VerifyScopedCanonicalState(); err != nil {
		return 0, nil, fmt.Errorf("verify %s scoped state: %w", label, err)
	}
	return uint64(state.Height), computed, nil // #nosec G115 -- positive int64 checked above
}
