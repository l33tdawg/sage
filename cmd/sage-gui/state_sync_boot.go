package main

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/cometbft/cometbft/config"
	cmtstate "github.com/cometbft/cometbft/state"
)

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
	if state.IsEmpty() || state.LastBlockHeight == 0 {
		if state.LastBlockHeight < 0 || len(state.AppHash) != 0 {
			return 0, nil, errors.New("empty CometBFT state has an invalid height or AppHash")
		}
		return 0, nil, nil
	}
	if state.LastBlockHeight < 0 || len(state.AppHash) != sha256.Size {
		return 0, nil, errors.New("persisted CometBFT state has an invalid height or AppHash")
	}
	return uint64(state.LastBlockHeight), append([]byte(nil), state.AppHash...), nil // #nosec G115 -- positive int64 checked above
}
