package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	cmtnode "github.com/cometbft/cometbft/node"
	cmtstate "github.com/cometbft/cometbft/state"
	cmtstore "github.com/cometbft/cometbft/store"
	cmttypes "github.com/cometbft/cometbft/types"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/statesync"
)

const stateSyncActivationJournalName = "state-sync-activation.journal"

type persistedCometStateReader func(*config.Config) (uint64, []byte, error)

var errStateSyncBootstrapCommitMissing = errors.New("state-sync bootstrap commit is not persisted")
var errStateSyncBlockSyncHandoffMissing = errors.New("state-sync block-sync handoff is not persisted")

type cometBootstrapCommitStore interface {
	LoadSeenCommit(int64) *cmttypes.Commit
	LoadBlockCommit(int64) *cmttypes.Commit
}

type cometStateSyncCompletionStore interface {
	LoadStateSyncBootstrapComplete() (int64, []byte, error)
}

type normalServingRuntime interface {
	AcquireNormalServingApplication() (abcitypes.Application, error)
}

type stateSyncSealRuntime interface {
	Phase() sageabci.BootStateSyncPhase
	ExpectedState() (int64, []byte)
	SealActivatedBundleFromComet(
		context.Context,
		func() (height int64, appHash []byte, appVersion uint64, err error),
	) (bool, int64, []byte, uint64, error)
}

type stateSyncSealResult struct {
	activated  bool
	height     int64
	appHash    []byte
	appVersion uint64
}

// readPersistedCometState reads the state DB before node.NewNode starts and
// before the ABCI handshake can compare CometBFT with SAGE. State-sync journal
// recovery uses this narrow seam to avoid booting an app whose Badger height is
// ahead of CometBFT after a crash between directory activation and Comet's own
// state persistence.
func readPersistedCometState(cfg *config.Config) (uint64, []byte, error) {
	return readPersistedCometStateWithProvider(cfg, config.DefaultDBProvider)
}

func readPersistedCometStateWithProvider(cfg *config.Config, provider config.DBProvider) (uint64, []byte, error) {
	if cfg == nil || provider == nil {
		return 0, nil, errors.New("CometBFT config and DB provider are required")
	}
	db, err := provider(&config.DBContext{ID: "state", Config: cfg})
	if err != nil {
		return 0, nil, fmt.Errorf("open CometBFT state DB: %w", err)
	}
	state, loadErr := cmtstate.NewStore(db, cmtstate.StoreOptions{}).Load()
	closeErr := db.Close()
	if loadErr != nil {
		return 0, nil, fmt.Errorf("load CometBFT state DB: %w", loadErr)
	}
	if closeErr != nil {
		return 0, nil, fmt.Errorf("close CometBFT state DB: %w", closeErr)
	}
	if state.IsEmpty() {
		if state.LastBlockHeight != 0 || len(state.AppHash) != 0 {
			return 0, nil, errors.New("empty CometBFT state has an invalid height or AppHash")
		}
		return 0, nil, nil
	}
	// A height-zero state with a cached genesis is not pristine. CometBFT would
	// prefer that persisted chain/validator state over the configured genesis on
	// restart, so a receiver must reject it rather than validating the wrong file.
	if state.LastBlockHeight <= 0 || len(state.AppHash) != sha256.Size {
		return 0, nil, errors.New("persisted CometBFT state has an invalid height or AppHash")
	}
	blockDB, err := provider(&config.DBContext{ID: "blockstore", Config: cfg})
	if err != nil {
		return 0, nil, fmt.Errorf("open CometBFT block store: %w", err)
	}
	blockStore := cmtstore.NewBlockStore(blockDB)
	commitErr := validateCometBootstrapCommit(state, blockStore)
	blockCloseErr := blockStore.Close()
	if commitErr != nil {
		return 0, nil, fmt.Errorf("validate persisted CometBFT bootstrap commit: %w", commitErr)
	}
	if blockCloseErr != nil {
		return 0, nil, fmt.Errorf("close CometBFT block store: %w", blockCloseErr)
	}
	return uint64(state.LastBlockHeight), append([]byte(nil), state.AppHash...), nil // #nosec G115 -- positive int64 checked above
}

// newRunningCometStateReader captures the node-owned StateStore once. Calling
// ConfigureRPC on every startup poll would repeatedly rebuild RPC/genesis
// plumbing while Comet is bootstrapping state sync.
func newRunningCometStateReader(cometNode *cmtnode.Node) (func(int64, []byte) (int64, []byte, uint64, error), error) {
	if cometNode == nil {
		return nil, errors.New("running CometBFT node is required")
	}
	blockStore := cometNode.BlockStore()
	if blockStore == nil {
		return nil, errors.New("running CometBFT block store is required")
	}
	var initOnce sync.Once
	var initErr error
	var loadState func() (cmtstate.State, error)
	return func(expectedHeight int64, expectedAppHash []byte) (int64, []byte, uint64, error) {
		initOnce.Do(func() {
			env, configureErr := cometNode.ConfigureRPC()
			if configureErr != nil {
				initErr = fmt.Errorf("configure CometBFT state reader: %w", configureErr)
				return
			}
			loadState = env.StateStore.Load
		})
		if initErr != nil {
			return 0, nil, 0, initErr
		}
		state, loadErr := loadState()
		if loadErr != nil {
			return 0, nil, 0, fmt.Errorf("load running CometBFT state: %w", loadErr)
		}
		height, appHash, appVersion, stateErr := runningCometState(state)
		if stateErr != nil || height == 0 {
			return height, appHash, appVersion, stateErr
		}
		if commitErr := validateCometBootstrapCommit(state, blockStore); commitErr != nil {
			if errors.Is(commitErr, errStateSyncBootstrapCommitMissing) {
				return 0, nil, 0, sageabci.ErrBootStateSyncPersistencePending
			}
			return 0, nil, 0, fmt.Errorf("validate running CometBFT bootstrap commit: %w", commitErr)
		}
		if handoffErr := validateCometStateSyncHandoff(expectedHeight, expectedAppHash, blockStore); handoffErr != nil {
			if errors.Is(handoffErr, errStateSyncBlockSyncHandoffMissing) {
				return 0, nil, 0, sageabci.ErrBootStateSyncPersistencePending
			}
			return 0, nil, 0, fmt.Errorf("validate running CometBFT block-sync handoff: %w", handoffErr)
		}
		return height, appHash, appVersion, nil
	}, nil
}

func validateCometBootstrapCommit(state cmtstate.State, blockStore cometBootstrapCommitStore) error {
	if blockStore == nil || state.LastBlockHeight <= 0 || state.ChainID == "" || state.LastValidators == nil {
		return errors.New("CometBFT bootstrap state or block store is incomplete")
	}
	commit := blockStore.LoadSeenCommit(state.LastBlockHeight)
	if commit == nil {
		commit = blockStore.LoadBlockCommit(state.LastBlockHeight)
	}
	if commit == nil {
		return errStateSyncBootstrapCommitMissing
	}
	if err := commit.ValidateBasic(); err != nil {
		return fmt.Errorf("bootstrap commit is malformed: %w", err)
	}
	if commit.Height != state.LastBlockHeight {
		return errors.New("bootstrap commit height does not match persisted state")
	}
	if err := state.LastValidators.VerifyCommit(state.ChainID, commit.BlockID, state.LastBlockHeight, commit); err != nil {
		return fmt.Errorf("bootstrap commit lacks a valid +2/3 signature: %w", err)
	}
	return nil
}

func validateCometStateSyncHandoff(expectedHeight int64, expectedAppHash []byte, blockStore cometStateSyncCompletionStore) error {
	if blockStore == nil || expectedHeight <= 0 || len(expectedAppHash) != sha256.Size {
		return errors.New("activated snapshot or CometBFT block store is incomplete")
	}
	height, appHash, err := blockStore.LoadStateSyncBootstrapComplete()
	if err != nil {
		return fmt.Errorf("load state-sync block-sync handoff: %w", err)
	}
	if height == 0 && len(appHash) == 0 {
		return errStateSyncBlockSyncHandoffMissing
	}
	if height != expectedHeight || !bytes.Equal(appHash, expectedAppHash) {
		return errors.New("state-sync block-sync handoff does not match the activated snapshot")
	}
	return nil
}

func runningCometState(state cmtstate.State) (int64, []byte, uint64, error) {
	if state.LastBlockHeight == 0 && len(state.AppHash) == 0 {
		return 0, nil, state.Version.Consensus.App, nil
	}
	if state.LastBlockHeight <= 0 || len(state.AppHash) != sha256.Size || state.Version.Consensus.App == 0 {
		return 0, nil, 0, errors.New("running CometBFT state has an invalid height, AppHash, or app version")
	}
	return state.LastBlockHeight, append([]byte(nil), state.AppHash...), state.Version.Consensus.App, nil
}

// waitForBootStateSyncSeal keeps all ordinary service admission closed while
// Comet discovers/assembles a snapshot and while its state-store persistence
// catches up to the activated application. The caller supplies a bounded
// context derived from quorum.state_sync.startup_timeout.
func waitForBootStateSyncSeal(
	ctx context.Context,
	runtime stateSyncSealRuntime,
	readComet func(expectedHeight int64, expectedAppHash []byte) (height int64, appHash []byte, appVersion uint64, err error),
) (stateSyncSealResult, error) {
	return waitForBootStateSyncSealWithInterval(ctx, runtime, readComet, 25*time.Millisecond)
}

func waitForBootStateSyncSealWithInterval(
	ctx context.Context,
	runtime stateSyncSealRuntime,
	readComet func(expectedHeight int64, expectedAppHash []byte) (height int64, appHash []byte, appVersion uint64, err error),
	pollInterval time.Duration,
) (stateSyncSealResult, error) {
	if runtime == nil || readComet == nil || pollInterval <= 0 {
		return stateSyncSealResult{}, errors.New("state sync seal runtime, Comet reader, and positive poll interval are required")
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		phase := runtime.Phase()
		switch phase {
		case sageabci.BootStateSyncDisabled, sageabci.BootStateSyncPendingComet:
			expectedHeight, expectedAppHash := runtime.ExpectedState()
			sealed, height, appHash, appVersion, err := runtime.SealActivatedBundleFromComet(ctx, func() (int64, []byte, uint64, error) {
				return readComet(expectedHeight, expectedAppHash)
			})
			if err == nil {
				return stateSyncSealResult{
					activated: sealed,
					height:    height, appHash: append([]byte(nil), appHash...), appVersion: appVersion,
				}, nil
			}
			if !errors.Is(err, sageabci.ErrBootStateSyncPersistencePending) {
				return stateSyncSealResult{}, err
			}
		case sageabci.BootStateSyncDiscovering, sageabci.BootStateSyncAssembling, sageabci.BootStateSyncPrepared:
			// Comet state sync is still using the intentionally narrow ABCI path.
		case sageabci.BootStateSyncFailed:
			return stateSyncSealResult{}, errors.New("boot state sync failed before normal serving admission")
		default:
			return stateSyncSealResult{}, fmt.Errorf("unexpected boot state-sync phase %d before normal serving", phase)
		}

		select {
		case <-ctx.Done():
			return stateSyncSealResult{}, fmt.Errorf("wait for boot state-sync seal in phase %d: %w", runtime.Phase(), ctx.Err())
		case <-ticker.C:
		}
	}
}

// recoverPendingStateSyncActivation runs before startup creates or opens the
// canonical Badger directory. In particular, MkdirAll(badger) must not run
// first: after a crash between live->quarantine and prepared->live, recreating
// an empty live directory would turn a recoverable layout into an ambiguous one.
func recoverPendingStateSyncActivation(ctx context.Context, dataDir, cometHome, badgerPath string) (statesync.RecoveryAction, bool, error) {
	return recoverPendingStateSyncActivationWith(
		ctx,
		dataDir,
		cometHome,
		badgerPath,
		readPersistedCometState,
		func(path string) (uint64, []byte, error) {
			return sageabci.InspectStateSyncRecoveryDirectory(ctx, path)
		},
	)
}

func recoverPendingStateSyncActivationWith(
	ctx context.Context,
	dataDir, cometHome, badgerPath string,
	readComet persistedCometStateReader,
	inspect statesync.ActivationDirectoryInspector,
) (statesync.RecoveryAction, bool, error) {
	if dataDir == "" || cometHome == "" || badgerPath == "" || readComet == nil || inspect == nil {
		return 0, false, errors.New("state sync recovery paths, Comet reader, and directory inspector are required")
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return 0, false, contextErr
	}
	journalPath := filepath.Join(dataDir, stateSyncActivationJournalName)
	if _, err := os.Lstat(journalPath); errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	} else if err != nil {
		return 0, true, fmt.Errorf("inspect state sync activation journal: %w", err)
	}
	journal, err := statesync.LoadActivationJournal(journalPath)
	if err != nil {
		return 0, true, err
	}
	if journal.LiveName != filepath.Base(badgerPath) || filepath.Dir(badgerPath) != dataDir {
		return 0, true, errors.New("state sync activation journal does not target the canonical Badger directory")
	}
	cometCfg := config.DefaultConfig()
	cometCfg.SetRoot(cometHome)
	cometHeight, cometAppHash, err := readComet(cometCfg)
	if err != nil {
		return 0, true, err
	}
	action, err := statesync.RecoverActivationDirectories(dataDir, journalPath, cometHeight, cometAppHash, inspect)
	if err != nil {
		return 0, true, err
	}
	return action, true, nil
}

// finishRecoveredStateSyncRole turns off the one-shot receiver in memory only
// after journal recovery has cryptographically kept the activated application
// against persisted Comet state. This lets a crash after StateStore.Bootstrap
// resume as an ordinary synchronized node without requiring an operator to win
// a restart race and edit configuration first.
func finishRecoveredStateSyncRole(cfg *Config, found bool, action statesync.RecoveryAction) bool {
	if cfg == nil || !found || action != statesync.RecoveryKeepActivated || !cfg.Quorum.StateSync.Receiving {
		return false
	}
	cfg.Quorum.StateSync.Receiving = false
	return true
}

// acquireNormalServingAfterStateSyncRecovery is the final admission barrier
// between CometBFT bootstrap and all ordinary SAGE listeners/workers. Recovery
// must have completed before the canonical Badger store opened, and an armed
// boot state-sync runtime must have sealed after Comet persisted its state.
// A successful acquire freezes bundle replacement before concrete app/store
// pointers escape to those services.
func acquireNormalServingAfterStateSyncRecovery(recoveryComplete bool, runtime normalServingRuntime) (abcitypes.Application, error) {
	if !recoveryComplete {
		return nil, errors.New("state sync activation recovery has not completed")
	}
	if runtime == nil {
		return nil, errors.New("boot state-sync runtime is required")
	}
	application, err := runtime.AcquireNormalServingApplication()
	if err != nil {
		return nil, err
	}
	if application == nil {
		return nil, errors.New("boot state-sync runtime returned a nil normal-serving application")
	}
	return application, nil
}
