package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	genesisV23, genesisErr := readOnly.GetAppV23GenesisActivation()
	if genesisErr != nil {
		_ = readOnly.CloseBadger()
		return 0, nil, genesisErr
	}
	// A quarantine may legitimately be from any older supported app version.
	// Pre-app-v12 AppHashes include bookkeeping that SaveState changes after
	// FinalizeBlock, so they cannot be recomputed from the closed post-Commit DB.
	// The persisted height/AppHash are still compared exactly with Comet by the
	// recovery executor. Once app-v20 is active, apply its stronger narrow-hash
	// and scoped-state checks as well.
	if genesisV23 == nil &&
		(appV20 == nil || appV20.TargetAppVersion != 20 ||
			appV20.AppliedHeight <= 0 || state.Height <= appV20.AppliedHeight) {
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
	state, stateErr := LoadState(badgerStore)
	if stateErr != nil {
		return 0, nil, fmt.Errorf("load %s app state: %w", label, stateErr)
	}
	if state.Height <= 0 {
		return 0, nil, fmt.Errorf("%s app height must be positive", label)
	}
	if state.EpochNum != poe.EpochNumber(state.Height) {
		return 0, nil, fmt.Errorf("%s persisted epoch does not match height", label)
	}
	applied, appliedErr := badgerStore.GetAppliedUpgrade(appV20UpgradeName)
	if appliedErr != nil {
		return 0, nil, appliedErr
	}
	genesisV23, genesisErr := badgerStore.GetAppV23GenesisActivation()
	if genesisErr != nil {
		return 0, nil, fmt.Errorf("read %s app-v23 genesis activation: %w", label, genesisErr)
	}
	if genesisV23 != nil {
		if lineageErr := badgerStore.ValidateAppV23GenesisLineage(); lineageErr != nil {
			return 0, nil, fmt.Errorf("verify %s app-v23 genesis lineage: %w", label, lineageErr)
		}
		if policyErr := badgerStore.ValidateAppV23State(); policyErr != nil {
			return 0, nil, fmt.Errorf("verify %s app-v23 genesis state: %w", label, policyErr)
		}
		root, rootErr := badgerStore.GetAppV23Root()
		if rootErr != nil || root == nil ||
			root.Scope != genesisV23.Scope ||
			root.BootstrapDigest != genesisV23.BootstrapDigest {
			return 0, nil, fmt.Errorf("%s app-v23 genesis Root lineage is invalid", label)
		}
		domain, domainErr := badgerStore.GetState(governanceDelegationDomainStateKey)
		decodedScope, decodeErr := hex.DecodeString(genesisV23.Scope)
		if domainErr != nil || decodeErr != nil || len(decodedScope) != sha256.Size ||
			!bytes.Equal(domain, decodedScope) {
			return 0, nil, fmt.Errorf("%s app-v23 genesis governance domain is invalid", label)
		}
	} else if applied == nil || applied.TargetAppVersion != 20 ||
		applied.AppliedHeight <= 0 || state.Height <= applied.AppliedHeight {
		return 0, nil, errors.New("state sync state is not from an active post-app-v20 height")
	}
	computed, hashErr := badgerStore.ComputeAppHashExcludingBookkeeping()
	if hashErr != nil {
		return 0, nil, hashErr
	}
	if len(state.AppHash) != sha256.Size || !bytes.Equal(state.AppHash, computed) {
		return 0, nil, fmt.Errorf("%s persisted AppHash does not match Badger state", label)
	}
	probe := &SageApp{badgerStore: badgerStore}
	if _, governanceErr := probe.governanceDelegationDomain(); governanceErr != nil {
		return 0, nil, fmt.Errorf("verify %s governance delegation domain: %w", label, governanceErr)
	}
	if _, scopedErr := probe.VerifyScopedCanonicalState(); scopedErr != nil {
		return 0, nil, fmt.Errorf("verify %s scoped state: %w", label, scopedErr)
	}
	appV23, upgradeErr := badgerStore.GetAppliedUpgrade(appV23UpgradeName)
	if upgradeErr != nil {
		return 0, nil, fmt.Errorf("read %s app-v23 activation: %w", label, upgradeErr)
	}
	if genesisV23 != nil {
		if appV23 != nil {
			return 0, nil, fmt.Errorf(
				"%s has both app-v23 genesis and governed app-v23 activation records",
				label,
			)
		}
	} else if appV23 != nil {
		if appV23.Name != appV23UpgradeName || appV23.TargetAppVersion != 23 ||
			appV23.AppliedHeight <= 0 || state.Height <= appV23.AppliedHeight {
			return 0, nil, fmt.Errorf("%s has invalid active app-v23 record", label)
		}
		previousHeight := applied.AppliedHeight
		for _, predecessor := range []struct {
			version uint64
			name    string
		}{
			{version: 21, name: appV21UpgradeName},
			{version: 22, name: appV22UpgradeName},
		} {
			version, name := predecessor.version, predecessor.name
			record, readErr := badgerStore.GetAppliedUpgrade(name)
			if readErr != nil {
				return 0, nil, fmt.Errorf("read %s %s predecessor: %w", label, name, readErr)
			}
			if record == nil || record.Name != name || record.TargetAppVersion != version ||
				record.AppliedHeight <= previousHeight || record.AppliedHeight >= appV23.AppliedHeight {
				return 0, nil, fmt.Errorf("%s has invalid app-v23 predecessor %s", label, name)
			}
			previousHeight = record.AppliedHeight
		}
		if err := badgerStore.ValidateAppV23State(); err != nil {
			return 0, nil, fmt.Errorf("verify %s app-v23 state: %w", label, err)
		}
		migration, err := badgerStore.GetAppV23MigrationState()
		if err != nil {
			return 0, nil, fmt.Errorf("read %s app-v23 migration state: %w", label, err)
		}
		if migration == nil {
			return 0, nil, fmt.Errorf("verify %s app-v23 state: migration state is missing", label)
		}
	}
	appV24, upgradeErr := badgerStore.GetAppliedUpgrade(appV24UpgradeName)
	if upgradeErr != nil {
		return 0, nil, fmt.Errorf("read %s app-v24 activation: %w", label, upgradeErr)
	}
	if appV24 != nil {
		if appV24.Name != appV24UpgradeName || appV24.TargetAppVersion != 24 ||
			appV24.AppliedHeight <= 0 || state.Height <= appV24.AppliedHeight {
			return 0, nil, fmt.Errorf("%s has invalid active app-v24 record", label)
		}
		switch {
		case genesisV23 != nil:
			// A direct-v23 genesis marker is the authenticated immediate
			// predecessor; there is intentionally no applied app-v23 record.
		case appV23 == nil:
			return 0, nil, fmt.Errorf("%s app-v24 activation is missing app-v23 predecessor", label)
		case appV24.AppliedHeight <= appV23.AppliedHeight:
			return 0, nil, fmt.Errorf("%s app-v24 activation does not follow app-v23", label)
		}
	}
	appV25, upgradeErr := badgerStore.GetAppliedUpgrade(appV25UpgradeName)
	if upgradeErr != nil {
		return 0, nil, fmt.Errorf("read %s app-v25 activation: %w", label, upgradeErr)
	}
	if appV25 != nil {
		if appV25.Name != appV25UpgradeName || appV25.TargetAppVersion != 25 ||
			appV25.AppliedHeight <= 0 || state.Height <= appV25.AppliedHeight {
			return 0, nil, fmt.Errorf("%s has invalid active app-v25 record", label)
		}
		if appV24 == nil {
			return 0, nil, fmt.Errorf("%s app-v25 activation is missing app-v24 predecessor", label)
		}
		if appV25.AppliedHeight <= appV24.AppliedHeight {
			return 0, nil, fmt.Errorf("%s app-v25 activation does not follow app-v24", label)
		}
	}
	appV26, upgradeErr := badgerStore.GetAppliedUpgrade(appV26UpgradeName)
	if upgradeErr != nil {
		return 0, nil, fmt.Errorf("read %s app-v26 activation: %w", label, upgradeErr)
	}
	if appV26 != nil {
		if appV26.Name != appV26UpgradeName || appV26.TargetAppVersion != 26 ||
			appV26.AppliedHeight <= 0 || state.Height < appV26.AppliedHeight {
			return 0, nil, fmt.Errorf("%s has invalid active app-v26 record", label)
		}
		if appV25 == nil {
			return 0, nil, fmt.Errorf("%s app-v26 activation is missing app-v25 predecessor", label)
		}
		if appV26.AppliedHeight <= appV25.AppliedHeight {
			return 0, nil, fmt.Errorf("%s app-v26 activation does not follow app-v25", label)
		}
		if groupErr := badgerStore.ValidateAppV26AccessGroupAuthorities(); groupErr != nil {
			return 0, nil, fmt.Errorf("verify %s app-v26 Access Groups: %w", label, groupErr)
		}
		// app-v26 is the repair boundary for historical app-v25 home-domain
		// defects. A completed app-v26 image must therefore be strict: accepting
		// the pre-v26 compatibility validator here would let an invalid snapshot
		// strand a receiver before any later transaction could repair it.
		if rbacErr := badgerStore.ValidateAppV23State(); rbacErr != nil {
			return 0, nil, fmt.Errorf("verify %s app-v26 repaired local RBAC: %w", label, rbacErr)
		}
	}
	return uint64(state.Height), computed, nil // #nosec G115 -- positive int64 checked above
}
