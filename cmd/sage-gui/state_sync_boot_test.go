package main

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/cometbft/cometbft/config"
	cmtcrypto "github.com/cometbft/cometbft/crypto/ed25519"
	cmtstate "github.com/cometbft/cometbft/state"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stateSyncCometConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.SetRoot(t.TempDir())
	cfg.DBBackend = "goleveldb"
	cfg.DBPath = "data"
	return cfg
}

func savePersistedCometState(t *testing.T, cfg *config.Config, height int64, appHash []byte) {
	t.Helper()
	db, err := config.DefaultDBProvider(&config.DBContext{ID: "state", Config: cfg})
	require.NoError(t, err)
	pubKey := cmtcrypto.GenPrivKey().PubKey()
	genesis := &cmttypes.GenesisDoc{
		GenesisTime:     time.Unix(1, 0).UTC(),
		ChainID:         "state-sync-test",
		InitialHeight:   1,
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators: []cmttypes.GenesisValidator{{
			Address: pubKey.Address(), PubKey: pubKey, Power: 10, Name: "validator-a",
		}},
	}
	state, err := cmtstate.MakeGenesisState(genesis)
	require.NoError(t, err)
	state.LastBlockHeight = height
	state.AppHash = append([]byte(nil), appHash...)
	if height > 0 {
		state.LastValidators = state.Validators.Copy()
	}
	require.NoError(t, cmtstate.NewStore(db, cmtstate.StoreOptions{}).Save(state))
	require.NoError(t, db.Close())
}

func TestReadPersistedCometStateBeforeHandshake(t *testing.T) {
	cfg := stateSyncCometConfig(t)
	want := sha256.Sum256([]byte("persisted app hash"))
	savePersistedCometState(t, cfg, 42, want[:])

	height, appHash, err := readPersistedCometState(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), height)
	assert.Equal(t, want[:], appHash)

	appHash[0] ^= 0xff
	heightAgain, hashAgain, err := readPersistedCometState(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), heightAgain)
	assert.Equal(t, want[:], hashAgain, "callers receive a private AppHash copy")
}

func TestReadPersistedCometStateHandlesFreshAndInvalidDatabases(t *testing.T) {
	fresh := stateSyncCometConfig(t)
	height, appHash, err := readPersistedCometState(fresh)
	require.NoError(t, err)
	assert.Zero(t, height)
	assert.Nil(t, appHash)

	invalid := stateSyncCometConfig(t)
	savePersistedCometState(t, invalid, 9, []byte("short"))
	_, _, err = readPersistedCometState(invalid)
	require.ErrorContains(t, err, "invalid height or AppHash")

	_, _, err = readPersistedCometState(nil)
	require.ErrorContains(t, err, "required")
}
