package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cometbft/cometbft/config"
	cmtcrypto "github.com/cometbft/cometbft/crypto/ed25519"
	cmtstate "github.com/cometbft/cometbft/state"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/statesync"
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

	emptyButPositive := stateSyncCometConfig(t)
	db, err := config.DefaultDBProvider(&config.DBContext{ID: "state", Config: emptyButPositive})
	require.NoError(t, err)
	badHash := sha256.Sum256([]byte("empty positive state"))
	inconsistent := cmtstate.State{LastBlockHeight: 9, AppHash: badHash[:]}
	require.NoError(t, cmtstate.NewStore(db, cmtstate.StoreOptions{}).Save(inconsistent))
	require.NoError(t, db.Close())
	_, _, err = readPersistedCometState(emptyButPositive)
	require.Error(t, err, "an internally empty positive-height Comet state must fail closed")
}

func writeBootRecoveryTestState(t *testing.T, path string, height uint64, appHash []byte) {
	t.Helper()
	require.NoError(t, os.Mkdir(path, 0o700))
	encoded := make([]byte, 8+len(appHash))
	binary.BigEndian.PutUint64(encoded, height)
	copy(encoded[8:], appHash)
	require.NoError(t, os.WriteFile(filepath.Join(path, "state"), encoded, 0o600))
}

func inspectBootRecoveryTestState(path string) (uint64, []byte, error) {
	encoded, err := os.ReadFile(filepath.Join(path, "state")) //nolint:gosec // test-owned path
	if err != nil {
		return 0, nil, err
	}
	if len(encoded) < 8 {
		return 0, nil, os.ErrInvalid
	}
	return binary.BigEndian.Uint64(encoded), append([]byte(nil), encoded[8:]...), nil
}

func TestRecoverPendingStateSyncActivationRunsBeforeLiveDirectoryCreation(t *testing.T) {
	dataDir := t.TempDir()
	cometHome := filepath.Join(dataDir, "cometbft")
	require.NoError(t, os.Mkdir(cometHome, 0o700))
	oldHash := sha256.Sum256([]byte("old state"))
	newHash := sha256.Sum256([]byte("new state"))
	metadataHash := sha256.Sum256([]byte("metadata"))
	journal := statesync.ActivationJournal{
		Phase: statesync.ActivationPrepared, Height: 42, AppHash: newHash[:], MetadataHash: metadataHash[:],
		PreparedName: "state-sync-prepared-42", QuarantineName: "badger-old-42", LiveName: "badger",
	}
	journalPath := filepath.Join(dataDir, stateSyncActivationJournalName)
	require.NoError(t, statesync.WriteActivationJournal(journalPath, journal))
	writeBootRecoveryTestState(t, filepath.Join(dataDir, journal.PreparedName), journal.Height, newHash[:])
	writeBootRecoveryTestState(t, filepath.Join(dataDir, journal.QuarantineName), 40, oldHash[:])
	badgerPath := filepath.Join(dataDir, journal.LiveName)
	assert.NoDirExists(t, badgerPath, "precondition models a crash after live was quarantined")

	action, found, err := recoverPendingStateSyncActivationWith(
		context.Background(), dataDir, cometHome, badgerPath,
		func(*config.Config) (uint64, []byte, error) { return 40, oldHash[:], nil },
		inspectBootRecoveryTestState,
	)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, statesync.RecoveryRestoreQuarantine, action)
	height, appHash, err := inspectBootRecoveryTestState(badgerPath)
	require.NoError(t, err)
	assert.Equal(t, uint64(40), height)
	assert.Equal(t, oldHash[:], appHash)
	assert.NoDirExists(t, filepath.Join(dataDir, journal.PreparedName))
	assert.NoDirExists(t, filepath.Join(dataDir, journal.QuarantineName))
	assert.NoFileExists(t, journalPath)
}

func TestRecoverPendingStateSyncActivationNoJournalAndCanonicalTarget(t *testing.T) {
	dataDir := t.TempDir()
	cometHome := filepath.Join(dataDir, "cometbft")
	require.NoError(t, os.Mkdir(cometHome, 0o700))
	readCalled := false
	action, found, err := recoverPendingStateSyncActivationWith(
		context.Background(), dataDir, cometHome, filepath.Join(dataDir, "badger"),
		func(*config.Config) (uint64, []byte, error) {
			readCalled = true
			return 0, nil, nil
		},
		inspectBootRecoveryTestState,
	)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, action)
	assert.False(t, readCalled, "Comet DB stays untouched when no recovery journal exists")
}
