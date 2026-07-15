package state

import (
	"testing"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
)

func TestBootstrapAtomicallyPersistsEffectiveStateSyncHeight(t *testing.T) {
	db := dbm.NewMemDB()
	store := NewStore(db, StoreOptions{})
	validators := types.NewValidatorSet([]*types.Validator{
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
	})
	state := State{
		ChainID:                          "sage-state-sync-bootstrap",
		InitialHeight:                    1,
		LastBlockHeight:                  42,
		LastValidators:                   validators.Copy(),
		Validators:                       validators.Copy(),
		NextValidators:                   validators.Copy(),
		LastHeightValidatorsChanged:      1,
		ConsensusParams:                  *types.DefaultConsensusParams(),
		LastHeightConsensusParamsChanged: 1,
		AppHash:                          make([]byte, 32),
	}

	require.NoError(t, store.Bootstrap(state))
	loaded, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, state.LastBlockHeight, loaded.LastBlockHeight)
	height, err := store.GetOfflineStateSyncHeight()
	require.NoError(t, err)
	require.Equal(t, state.LastBlockHeight, height)
}
