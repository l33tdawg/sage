package statesync

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ActivationDirectoryVerifier opens the newly named live database, verifies
// its exact consensus height and AppHash, and closes it before returning. The
// executor never publishes a database handle or starts ordinary services.
type ActivationDirectoryVerifier func(path string, height uint64, appHash []byte) error

// ActivationAuthorizationGuard revalidates the one-shot join authorization at
// a durable activation boundary. Once it fails, callers must latch that failure
// and never permit clock rollback to re-enable the session.
type ActivationAuthorizationGuard func() error

// ActivationDirectoryInspector returns the consensus height and AppHash held
// by one closed, real directory. Recovery verifies this result against the
// already persisted CometBFT state before it renames or removes anything.
type ActivationDirectoryInspector func(path string) (height uint64, appHash []byte, err error)

type activationDirectoryPaths struct {
	root       string
	journal    string
	prepared   string
	quarantine string
	live       string
}

type activationDirectoryLayout struct {
	prepared   bool
	quarantine bool
	live       bool
}

type activationDirectoryStep string

const (
	activationStepJournalPrepared activationDirectoryStep = "journal_prepared"
	activationStepLiveQuarantined activationDirectoryStep = "live_quarantined"
	activationStepPreparedLive    activationDirectoryStep = "prepared_live"
	activationStepVerified        activationDirectoryStep = "verified"
	activationStepPendingComet    activationDirectoryStep = "pending_comet"
)

type activationDirectoryHook func(activationDirectoryStep) error

// ActivatePreparedDirectory performs the durable, boot-only directory portion
// of state-sync activation. journal must be in ActivationPrepared, the journal
// file must not already exist, and all three directory names must be direct
// children of root. The returned live directory is not safe to serve until
// CometBFT persists the synchronized state and recovery seals the journal.
func ActivatePreparedDirectory(root, journalPath string, journal ActivationJournal, verify ActivationDirectoryVerifier) error {
	return activatePreparedDirectory(root, journalPath, journal, verify, nil)
}

func activatePreparedDirectory(root, journalPath string, journal ActivationJournal, verify ActivationDirectoryVerifier, hook activationDirectoryHook) error {
	return activatePreparedDirectoryAuthorized(root, journalPath, journal, verify, nil, hook)
}

// ActivatePreparedDirectoryAuthorized performs the same crash-safe directory
// transaction while rechecking authorization before each journal or rename.
// An authorization failure rolls the directory layout back to the old live
// state whenever any durable step has already occurred.
func ActivatePreparedDirectoryAuthorized(
	root, journalPath string,
	journal ActivationJournal,
	verify ActivationDirectoryVerifier,
	authorize ActivationAuthorizationGuard,
) error {
	if authorize == nil {
		return errors.New("activation authorization guard is required")
	}
	return activatePreparedDirectoryAuthorized(root, journalPath, journal, verify, authorize, nil)
}

func activatePreparedDirectoryAuthorized(
	root, journalPath string,
	journal ActivationJournal,
	verify ActivationDirectoryVerifier,
	authorize ActivationAuthorizationGuard,
	hook activationDirectoryHook,
) error {
	if verify == nil {
		return errors.New("activation directory verifier is required")
	}
	if journal.Phase != ActivationPrepared {
		return errors.New("activation directory transaction requires prepared phase")
	}
	paths, resolveErr := resolveActivationDirectoryPaths(root, journalPath, journal)
	if resolveErr != nil {
		return resolveErr
	}
	if err := requireMissingPath(paths.journal, "activation journal"); err != nil {
		return err
	}
	layout, err := inspectActivationDirectoryLayout(paths)
	if err != nil {
		return err
	}
	if !layout.live || !layout.prepared || layout.quarantine {
		return errors.New("activation requires live and prepared directories with no quarantine")
	}
	if err := runActivationAuthorizationGuard(authorize); err != nil {
		return err
	}

	if err := WriteActivationJournal(paths.journal, journal); err != nil {
		return fmt.Errorf("write prepared activation journal: %w", err)
	}
	if err := runActivationDirectoryHook(hook, activationStepJournalPrepared); err != nil {
		return err
	}
	if err := runActivationAuthorizationGuard(authorize); err != nil {
		return rollbackActivationAuthorizationFailure(paths, err)
	}
	if err := os.Rename(paths.live, paths.quarantine); err != nil {
		return fmt.Errorf("quarantine live activation directory: %w", err)
	}
	if err := syncDir(paths.root); err != nil {
		return fmt.Errorf("sync quarantined activation directory: %w", err)
	}
	if err := runActivationDirectoryHook(hook, activationStepLiveQuarantined); err != nil {
		return err
	}
	if err := runActivationAuthorizationGuard(authorize); err != nil {
		return rollbackActivationAuthorizationFailure(paths, err)
	}
	if err := os.Rename(paths.prepared, paths.live); err != nil {
		return fmt.Errorf("promote prepared activation directory: %w", err)
	}
	if err := syncDir(paths.root); err != nil {
		return fmt.Errorf("sync promoted activation directory: %w", err)
	}
	if err := runActivationDirectoryHook(hook, activationStepPreparedLive); err != nil {
		return err
	}
	if err := runActivationAuthorizationGuard(authorize); err != nil {
		return rollbackActivationAuthorizationFailure(paths, err)
	}

	if err := verify(paths.live, journal.Height, append([]byte(nil), journal.AppHash...)); err != nil {
		verifyErr := fmt.Errorf("verify promoted activation directory: %w", err)
		if rollbackErr := rollbackActivatedDirectory(paths); rollbackErr != nil {
			return errors.Join(verifyErr, fmt.Errorf("rollback promoted activation directory: %w", rollbackErr))
		}
		if cleanupErr := removeActivationJournal(paths.journal); cleanupErr != nil {
			return errors.Join(verifyErr, fmt.Errorf("remove rolled-back activation journal: %w", cleanupErr))
		}
		return verifyErr
	}
	if err := runActivationDirectoryHook(hook, activationStepVerified); err != nil {
		return err
	}
	if err := runActivationAuthorizationGuard(authorize); err != nil {
		return rollbackActivationAuthorizationFailure(paths, err)
	}

	journal.Phase = ActivationPendingComet
	if err := WriteActivationJournal(paths.journal, journal); err != nil {
		return fmt.Errorf("write pending-Comet activation journal: %w", err)
	}
	if err := runActivationDirectoryHook(hook, activationStepPendingComet); err != nil {
		return err
	}
	return nil
}

// RecoverActivationDirectories resolves a durable activation journal before
// the ABCI handshake. It either restores the store matching persisted CometBFT
// state or seals the activated store; ambiguous layouts are left untouched.
func RecoverActivationDirectories(root, journalPath string, cometHeight uint64, cometAppHash []byte, inspect ActivationDirectoryInspector) (RecoveryAction, error) {
	if inspect == nil {
		return 0, errors.New("activation directory inspector is required")
	}
	if !validPersistedState(cometHeight, cometAppHash) {
		return 0, errors.New("persisted CometBFT state hash is invalid")
	}
	journal, err := LoadActivationJournal(journalPath)
	if err != nil {
		return 0, fmt.Errorf("load activation journal: %w", err)
	}
	paths, err := resolveActivationDirectoryPaths(root, journalPath, journal)
	if err != nil {
		return 0, err
	}
	layout, err := inspectActivationDirectoryLayout(paths)
	if err != nil {
		return 0, err
	}

	switch journal.Phase {
	case ActivationPrepared:
		return recoverPreparedActivation(paths, layout, cometHeight, cometAppHash, inspect)
	case ActivationPendingComet:
		return recoverPendingActivation(paths, layout, journal, cometHeight, cometAppHash, inspect)
	case ActivationSealed:
		return recoverSealedActivation(paths, layout, journal, cometHeight, cometAppHash, inspect)
	default:
		return 0, errors.New("activation journal phase is invalid")
	}
}

// SealActivatedDirectory completes an activation while the promoted live
// database is already open. It deliberately does not inspect or reopen live:
// callers must first verify that persisted CometBFT and the active application
// report activeHeight/activeAppHash exactly. The durable PendingComet journal
// and directory layout remain the independent evidence that quarantine can be
// removed. A crash after the sealed write is resolved by boot recovery.
func SealActivatedDirectory(
	root, journalPath string,
	cometHeight uint64,
	cometAppHash []byte,
	activeHeight uint64,
	activeAppHash []byte,
) error {
	if !validPersistedState(cometHeight, cometAppHash) || !validPersistedState(activeHeight, activeAppHash) {
		return errors.New("activation seal state hash is invalid")
	}
	if cometHeight != activeHeight || !bytes.Equal(cometAppHash, activeAppHash) {
		return errors.New("active application state does not match persisted CometBFT state")
	}
	j, err := LoadActivationJournal(journalPath)
	if err != nil {
		return fmt.Errorf("load pending-Comet activation journal: %w", err)
	}
	if j.Phase != ActivationPendingComet {
		return errors.New("live activation seal requires pending-Comet journal phase")
	}
	paths, err := resolveActivationDirectoryPaths(root, journalPath, j)
	if err != nil {
		return err
	}
	layout, err := inspectActivationDirectoryLayout(paths)
	if err != nil {
		return err
	}
	if !layout.live || layout.prepared || !layout.quarantine {
		return errors.New("pending activation seal layout is ambiguous")
	}
	if cometHeight < j.Height {
		return errors.New("persisted CometBFT state is below pending activation")
	}
	if cometHeight == j.Height && !bytes.Equal(cometAppHash, j.AppHash) {
		return errors.New("persisted CometBFT state conflicts with pending activation")
	}
	return sealActivationDirectory(paths, j, true)
}

func recoverPreparedActivation(paths activationDirectoryPaths, layout activationDirectoryLayout, cometHeight uint64, cometAppHash []byte, inspect ActivationDirectoryInspector) (RecoveryAction, error) {
	switch {
	case layout.live && layout.prepared && !layout.quarantine:
		if err := inspectActivationState(inspect, paths.live, cometHeight, cometAppHash); err != nil {
			return 0, err
		}
		if err := removeActivationDirectory(paths.root, paths.prepared); err != nil {
			return 0, err
		}
		if err := removeActivationJournal(paths.journal); err != nil {
			return 0, err
		}
		return RecoveryDiscardPrepared, nil

	case layout.live && !layout.prepared && !layout.quarantine:
		// Recovery itself may have restored live and removed prepared before
		// crashing prior to journal removal.
		if err := inspectActivationState(inspect, paths.live, cometHeight, cometAppHash); err != nil {
			return 0, err
		}
		if err := removeActivationJournal(paths.journal); err != nil {
			return 0, err
		}
		return RecoveryDiscardPrepared, nil

	case !layout.live && layout.prepared && layout.quarantine:
		if err := inspectActivationState(inspect, paths.quarantine, cometHeight, cometAppHash); err != nil {
			return 0, err
		}
		if err := restoreQuarantineAndDiscardPrepared(paths, false); err != nil {
			return 0, err
		}
		return RecoveryRestoreQuarantine, nil

	case layout.live && !layout.prepared && layout.quarantine:
		if err := inspectActivationState(inspect, paths.quarantine, cometHeight, cometAppHash); err != nil {
			return 0, err
		}
		if err := restoreQuarantineAndDiscardPrepared(paths, true); err != nil {
			return 0, err
		}
		return RecoveryRestoreQuarantine, nil

	default:
		return 0, errors.New("prepared activation directory layout is ambiguous")
	}
}

func recoverPendingActivation(paths activationDirectoryPaths, layout activationDirectoryLayout, journal ActivationJournal, cometHeight uint64, cometAppHash []byte, inspect ActivationDirectoryInspector) (RecoveryAction, error) {
	if cometHeight < journal.Height {
		switch {
		case layout.live && !layout.prepared && layout.quarantine:
			if err := inspectActivationState(inspect, paths.quarantine, cometHeight, cometAppHash); err != nil {
				return 0, err
			}
			if err := restoreQuarantineAndDiscardPrepared(paths, true); err != nil {
				return 0, err
			}
			return RecoveryRestoreQuarantine, nil

		case !layout.live && layout.prepared && layout.quarantine:
			if err := inspectActivationState(inspect, paths.quarantine, cometHeight, cometAppHash); err != nil {
				return 0, err
			}
			if err := restoreQuarantineAndDiscardPrepared(paths, false); err != nil {
				return 0, err
			}
			return RecoveryRestoreQuarantine, nil

		case layout.live && layout.prepared && !layout.quarantine:
			if err := inspectActivationState(inspect, paths.live, cometHeight, cometAppHash); err != nil {
				return 0, err
			}
			if err := removeActivationDirectory(paths.root, paths.prepared); err != nil {
				return 0, err
			}
			if err := removeActivationJournal(paths.journal); err != nil {
				return 0, err
			}
			return RecoveryRestoreQuarantine, nil

		case layout.live && !layout.prepared && !layout.quarantine:
			if err := inspectActivationState(inspect, paths.live, cometHeight, cometAppHash); err != nil {
				return 0, err
			}
			if err := removeActivationJournal(paths.journal); err != nil {
				return 0, err
			}
			return RecoveryRestoreQuarantine, nil

		default:
			return 0, errors.New("pending activation rollback layout is ambiguous")
		}
	}

	if !layout.live || layout.prepared || !layout.quarantine {
		return 0, errors.New("pending activation seal layout is ambiguous")
	}
	liveHeight, liveHash, err := inspectActivationDirectory(inspect, paths.live)
	if err != nil {
		return 0, err
	}
	action, err := DecideActivationRecovery(journal, cometHeight, cometAppHash, liveHeight, liveHash)
	if err != nil {
		return 0, err
	}
	if action != RecoveryKeepActivated {
		return 0, errors.New("pending activation cannot be sealed")
	}
	if err := sealActivationDirectory(paths, journal, true); err != nil {
		return 0, err
	}
	return RecoveryKeepActivated, nil
}

func recoverSealedActivation(paths activationDirectoryPaths, layout activationDirectoryLayout, journal ActivationJournal, cometHeight uint64, cometAppHash []byte, inspect ActivationDirectoryInspector) (RecoveryAction, error) {
	if !layout.live || layout.prepared {
		return 0, errors.New("sealed activation directory layout is ambiguous")
	}
	liveHeight, liveHash, err := inspectActivationDirectory(inspect, paths.live)
	if err != nil {
		return 0, err
	}
	action, err := DecideActivationRecovery(journal, cometHeight, cometAppHash, liveHeight, liveHash)
	if err != nil {
		return 0, err
	}
	if action != RecoveryKeepActivated {
		return 0, errors.New("sealed activation cannot be kept")
	}
	if err := sealActivationDirectory(paths, journal, layout.quarantine); err != nil {
		return 0, err
	}
	return RecoveryKeepActivated, nil
}

func sealActivationDirectory(paths activationDirectoryPaths, journal ActivationJournal, removeQuarantine bool) error {
	if journal.Phase != ActivationSealed {
		journal.Phase = ActivationSealed
		if err := WriteActivationJournal(paths.journal, journal); err != nil {
			return fmt.Errorf("write sealed activation journal: %w", err)
		}
	}
	if removeQuarantine {
		if err := removeActivationDirectory(paths.root, paths.quarantine); err != nil {
			return err
		}
	}
	if err := removeActivationJournal(paths.journal); err != nil {
		return err
	}
	return nil
}

func rollbackActivatedDirectory(paths activationDirectoryPaths) error {
	layout, err := inspectActivationDirectoryLayout(paths)
	if err != nil {
		return err
	}
	if !layout.live || layout.prepared || !layout.quarantine {
		return errors.New("activated directory rollback layout is ambiguous")
	}
	return restoreQuarantineAndDiscardPrepared(paths, true)
}

func restoreQuarantineAndDiscardPrepared(paths activationDirectoryPaths, moveLiveAside bool) error {
	if moveLiveAside {
		if err := os.Rename(paths.live, paths.prepared); err != nil {
			return fmt.Errorf("move activated live directory aside: %w", err)
		}
		if err := syncDir(paths.root); err != nil {
			return fmt.Errorf("sync moved-aside activation directory: %w", err)
		}
	}
	if err := os.Rename(paths.quarantine, paths.live); err != nil {
		return fmt.Errorf("restore quarantined activation directory: %w", err)
	}
	if err := syncDir(paths.root); err != nil {
		return fmt.Errorf("sync restored activation directory: %w", err)
	}
	if err := removeActivationDirectory(paths.root, paths.prepared); err != nil {
		return err
	}
	if err := removeActivationJournal(paths.journal); err != nil {
		return err
	}
	return nil
}

func resolveActivationDirectoryPaths(root, journalPath string, journal ActivationJournal) (activationDirectoryPaths, error) {
	if err := validateActivationJournal(journal); err != nil {
		return activationDirectoryPaths{}, err
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return activationDirectoryPaths{}, err
	}
	rootInfo, err := os.Lstat(rootAbs)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return activationDirectoryPaths{}, errors.New("activation root must be an existing real directory")
	}
	journalAbs, err := filepath.Abs(journalPath)
	if err != nil {
		return activationDirectoryPaths{}, err
	}
	if filepath.Dir(journalAbs) != rootAbs {
		return activationDirectoryPaths{}, errors.New("activation journal must be a direct child of activation root")
	}
	journalName := filepath.Base(journalAbs)
	if journalName == journal.PreparedName || journalName == journal.QuarantineName || journalName == journal.LiveName {
		return activationDirectoryPaths{}, errors.New("activation journal conflicts with a state directory")
	}
	return activationDirectoryPaths{
		root:       rootAbs,
		journal:    journalAbs,
		prepared:   filepath.Join(rootAbs, journal.PreparedName),
		quarantine: filepath.Join(rootAbs, journal.QuarantineName),
		live:       filepath.Join(rootAbs, journal.LiveName),
	}, nil
}

func inspectActivationDirectoryLayout(paths activationDirectoryPaths) (activationDirectoryLayout, error) {
	prepared, err := realDirectoryExists(paths.prepared)
	if err != nil {
		return activationDirectoryLayout{}, fmt.Errorf("inspect prepared activation directory: %w", err)
	}
	quarantine, err := realDirectoryExists(paths.quarantine)
	if err != nil {
		return activationDirectoryLayout{}, fmt.Errorf("inspect quarantined activation directory: %w", err)
	}
	live, err := realDirectoryExists(paths.live)
	if err != nil {
		return activationDirectoryLayout{}, fmt.Errorf("inspect live activation directory: %w", err)
	}
	return activationDirectoryLayout{prepared: prepared, quarantine: quarantine, live: live}, nil
}

func realDirectoryExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("path must be a real directory or absent")
	}
	return true, nil
}

func requireMissingPath(path, label string) error {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%s already exists", label)
}

func inspectActivationState(inspect ActivationDirectoryInspector, path string, height uint64, appHash []byte) error {
	actualHeight, actualHash, err := inspectActivationDirectory(inspect, path)
	if err != nil {
		return err
	}
	if actualHeight != height || !bytes.Equal(actualHash, appHash) {
		return errors.New("activation directory state does not match persisted CometBFT state")
	}
	return nil
}

func inspectActivationDirectory(inspect ActivationDirectoryInspector, path string) (uint64, []byte, error) {
	height, appHash, err := inspect(path)
	if err != nil {
		return 0, nil, fmt.Errorf("inspect activation directory %q: %w", filepath.Base(path), err)
	}
	if !validPersistedState(height, appHash) {
		return 0, nil, fmt.Errorf("activation directory %q returned an invalid state hash", filepath.Base(path))
	}
	return height, append([]byte(nil), appHash...), nil
}

func removeActivationDirectory(root, path string) error {
	exists, err := realDirectoryExists(path)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if filepath.Dir(path) != root {
		return errors.New("activation cleanup directory escapes root")
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if err := syncDir(root); err != nil {
		return fmt.Errorf("sync activation directory cleanup: %w", err)
	}
	return nil
}

func removeActivationJournal(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("activation journal cleanup requires a regular file")
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync activation journal cleanup: %w", err)
	}
	return nil
}

func runActivationDirectoryHook(hook activationDirectoryHook, step activationDirectoryStep) error {
	if hook == nil {
		return nil
	}
	if err := hook(step); err != nil {
		return fmt.Errorf("activation interrupted after %s: %w", step, err)
	}
	return nil
}

func runActivationAuthorizationGuard(authorize ActivationAuthorizationGuard) error {
	if authorize == nil {
		return nil
	}
	if err := authorize(); err != nil {
		return fmt.Errorf("state sync activation authorization failed: %w", err)
	}
	return nil
}

func rollbackActivationAuthorizationFailure(paths activationDirectoryPaths, cause error) error {
	layout, err := inspectActivationDirectoryLayout(paths)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("inspect authorization rollback layout: %w", err))
	}
	switch {
	case layout.live && layout.prepared && !layout.quarantine:
		err = removeActivationJournal(paths.journal)
	case !layout.live && layout.prepared && layout.quarantine:
		err = restoreQuarantineAndDiscardPrepared(paths, false)
	case layout.live && !layout.prepared && layout.quarantine:
		err = rollbackActivatedDirectory(paths)
	default:
		err = errors.New("authorization rollback activation directory layout is ambiguous")
	}
	if err != nil {
		return errors.Join(cause, fmt.Errorf("rollback expired state sync activation: %w", err))
	}
	return cause
}
