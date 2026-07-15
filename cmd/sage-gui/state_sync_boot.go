package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cometbft/cometbft/config"
	cmtstate "github.com/cometbft/cometbft/state"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/statesync"
)

const stateSyncActivationJournalName = "state-sync-activation.journal"

type persistedCometStateReader func(*config.Config) (uint64, []byte, error)

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
	if state.LastBlockHeight == 0 {
		if len(state.AppHash) != 0 {
			return 0, nil, errors.New("empty CometBFT state has an invalid height or AppHash")
		}
		return 0, nil, nil
	}
	if state.IsEmpty() || state.LastBlockHeight < 0 || len(state.AppHash) != sha256.Size {
		return 0, nil, errors.New("persisted CometBFT state has an invalid height or AppHash")
	}
	return uint64(state.LastBlockHeight), append([]byte(nil), state.AppHash...), nil // #nosec G115 -- positive int64 checked above
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
