package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	cmtcrypto "github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/privval"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmtstate "github.com/cometbft/cometbft/state"
	cmtstore "github.com/cometbft/cometbft/store"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/statesync"
)

type stateSyncRecoveryTestApp struct{ abcitypes.BaseApplication }

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
	savePersistedCometStateFixture(t, cfg, height, appHash, true)
}

func savePersistedCometStateWithoutCommit(t *testing.T, cfg *config.Config, height int64, appHash []byte) {
	t.Helper()
	savePersistedCometStateFixture(t, cfg, height, appHash, false)
}

func savePersistedCometStateFixture(t *testing.T, cfg *config.Config, height int64, appHash []byte, persistCommit bool) {
	t.Helper()
	savePersistedCometStateFixtureWithKey(t, cfg, height, appHash, persistCommit, cmtcrypto.GenPrivKey())
}

func savePersistedCometStateFixtureWithKey(
	t *testing.T,
	cfg *config.Config,
	height int64,
	appHash []byte,
	persistCommit bool,
	privKey cmtcrypto.PrivKey,
) {
	t.Helper()
	db, err := config.DefaultDBProvider(&config.DBContext{ID: "state", Config: cfg})
	require.NoError(t, err)
	pubKey := privKey.PubKey()
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
	var blockID cmttypes.BlockID
	if height > 0 {
		state.LastValidators = state.Validators.Copy()
		blockHash := sha256.Sum256([]byte("state-sync-test-block"))
		partsHash := sha256.Sum256([]byte("state-sync-test-parts"))
		blockID = cmttypes.BlockID{
			Hash: blockHash[:],
			PartSetHeader: cmttypes.PartSetHeader{
				Total: 1,
				Hash:  partsHash[:],
			},
		}
		state.LastBlockID = blockID
	}
	require.NoError(t, cmtstate.NewStore(db, cmtstate.StoreOptions{}).Save(state))
	require.NoError(t, db.Close())
	if height > 0 && persistCommit {
		vote := &cmttypes.Vote{
			ValidatorAddress: pubKey.Address(),
			ValidatorIndex:   0,
			Height:           height,
			Round:            0,
			Type:             cmtproto.PrecommitType,
			BlockID:          blockID,
			Timestamp:        time.Unix(2, 0).UTC(),
		}
		protoVote := vote.ToProto()
		privValidator := cmttypes.NewMockPVWithParams(privKey, false, false)
		require.NoError(t, privValidator.SignVote(state.ChainID, protoVote))
		commit := &cmttypes.Commit{
			Height:  height,
			Round:   0,
			BlockID: blockID,
			Signatures: []cmttypes.CommitSig{{
				BlockIDFlag:      cmttypes.BlockIDFlagCommit,
				ValidatorAddress: pubKey.Address(),
				Timestamp:        vote.Timestamp,
				Signature:        append([]byte(nil), protoVote.Signature...),
			}},
		}
		blockDB, err := config.DefaultDBProvider(&config.DBContext{ID: "blockstore", Config: cfg})
		require.NoError(t, err)
		blockStore := cmtstore.NewBlockStore(blockDB)
		require.NoError(t, blockStore.SaveSeenCommit(height, commit))
		require.NoError(t, blockStore.Close())
	}
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

func TestStateSyncSealEvidenceDoesNotRequireBootstrapBlockBody(t *testing.T) {
	cfg := stateSyncCometConfig(t)
	wantHash := sha256.Sum256([]byte("state-sync application state at H"))
	savePersistedCometState(t, cfg, 97, wantHash[:])

	stateDB, err := config.DefaultDBProvider(&config.DBContext{ID: "state", Config: cfg})
	require.NoError(t, err)
	stateStore := cmtstate.NewStore(stateDB, cmtstate.StoreOptions{})
	state, err := stateStore.Load()
	require.NoError(t, err)
	state.Version.Consensus.App = 20
	require.NoError(t, stateStore.Save(state))
	require.NoError(t, stateDB.Close())

	height, appHash, err := readPersistedCometState(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint64(97), height)
	assert.Equal(t, wantHash[:], appHash)

	blockDB, err := config.DefaultDBProvider(&config.DBContext{ID: "blockstore", Config: cfg})
	require.NoError(t, err)
	blockStore := cmtstore.NewBlockStore(blockDB)
	assert.Zero(t, blockStore.Base())
	assert.Zero(t, blockStore.Height(), "Comet /status remains empty until block sync materializes H+1")
	assert.Nil(t, blockStore.LoadBlockMeta(97))
	assert.Nil(t, blockStore.LoadBlock(97))
	commit := blockStore.LoadSeenCommit(97)
	require.NotNil(t, commit)
	assert.True(t, commit.BlockID.Equals(state.LastBlockID))
	require.NoError(t, blockStore.SaveStateSyncBootstrapComplete(97, wantHash[:]))
	require.NoError(t, validateCometBootstrapCommit(state, blockStore))
	require.NoError(t, validateCometStateSyncHandoff(97, wantHash[:], blockStore))
	require.NoError(t, blockStore.Close())

	runningHeight, runningHash, appVersion, err := runningCometState(state)
	require.NoError(t, err)
	assert.Equal(t, int64(97), runningHeight)
	assert.Equal(t, wantHash[:], runningHash)
	assert.Equal(t, uint64(20), appVersion)
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

	cachedGenesis := stateSyncCometConfig(t)
	db, err = config.DefaultDBProvider(&config.DBContext{ID: "state", Config: cachedGenesis})
	require.NoError(t, err)
	genesisKey := cmtcrypto.GenPrivKey().PubKey()
	genesisState, err := cmtstate.MakeGenesisState(&cmttypes.GenesisDoc{
		GenesisTime: time.Unix(1, 0).UTC(), ChainID: "cached-genesis", InitialHeight: 1,
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators:      []cmttypes.GenesisValidator{{PubKey: genesisKey, Power: 10}},
	})
	require.NoError(t, err)
	require.NoError(t, cmtstate.NewStore(db, cmtstate.StoreOptions{}).Save(genesisState))
	require.NoError(t, db.Close())
	_, _, err = readPersistedCometState(cachedGenesis)
	require.ErrorContains(t, err, "invalid height or AppHash")

	missingCommit := stateSyncCometConfig(t)
	missingHash := sha256.Sum256([]byte("missing bootstrap commit"))
	savePersistedCometStateWithoutCommit(t, missingCommit, 11, missingHash[:])
	_, _, err = readPersistedCometState(missingCommit)
	require.ErrorIs(t, err, errStateSyncBootstrapCommitMissing)

	mismatchedCommit := stateSyncCometConfig(t)
	mismatchedHash := sha256.Sum256([]byte("mismatched bootstrap commit"))
	savePersistedCometState(t, mismatchedCommit, 12, mismatchedHash[:])
	db, err = config.DefaultDBProvider(&config.DBContext{ID: "state", Config: mismatchedCommit})
	require.NoError(t, err)
	stateStore := cmtstate.NewStore(db, cmtstate.StoreOptions{})
	state, err := stateStore.Load()
	require.NoError(t, err)
	wrongBlockHash := sha256.Sum256([]byte("wrong persisted block ID"))
	state.LastBlockID.Hash = wrongBlockHash[:]
	require.NoError(t, stateStore.Save(state))
	require.NoError(t, db.Close())
	_, _, err = readPersistedCometState(mismatchedCommit)
	require.ErrorContains(t, err, "block ID does not match persisted state")
}

func TestRunningCometStateAllowsCanonicalEmptyBootstrap(t *testing.T) {
	height, appHash, appVersion, err := runningCometState(cmtstate.State{})
	require.NoError(t, err)
	assert.Zero(t, height)
	assert.Nil(t, appHash)
	assert.Zero(t, appVersion)

	wantHash := sha256.Sum256([]byte("running Comet state"))
	state := cmtstate.State{LastBlockHeight: 42, AppHash: wantHash[:]}
	state.Version.Consensus.App = 20
	height, appHash, appVersion, err = runningCometState(state)
	require.NoError(t, err)
	assert.Equal(t, int64(42), height)
	assert.Equal(t, wantHash[:], appHash)
	assert.Equal(t, uint64(20), appVersion)

	_, _, _, err = runningCometState(cmtstate.State{LastBlockHeight: 0, AppHash: wantHash[:]})
	assert.ErrorContains(t, err, "invalid height")
}

func TestCometStateSyncReceiverMustBeAbsentFromEveryPersistedValidatorSet(t *testing.T) {
	receiverKey := cmtcrypto.GenPrivKey()
	otherKey := cmtcrypto.GenPrivKey()
	receiverSet := cmttypes.NewValidatorSet([]*cmttypes.Validator{
		cmttypes.NewValidator(receiverKey.PubKey(), 10),
	})
	otherSet := cmttypes.NewValidatorSet([]*cmttypes.Validator{
		cmttypes.NewValidator(otherKey.PubKey(), 10),
	})
	base := func() cmtstate.State {
		return cmtstate.State{
			LastBlockHeight: 42,
			LastValidators:  otherSet.Copy(),
			Validators:      otherSet.Copy(),
			NextValidators:  otherSet.Copy(),
		}
	}

	require.NoError(t, requireCometStateSyncReceiverNonValidator(base(), nil))
	require.NoError(t, requireCometStateSyncReceiverNonValidator(base(), receiverKey.PubKey().Bytes()))
	for _, tc := range []struct {
		name string
		set  func(*cmtstate.State)
		want string
	}{
		{name: "last", set: func(state *cmtstate.State) { state.LastValidators = receiverSet.Copy() }, want: "last"},
		{name: "current", set: func(state *cmtstate.State) { state.Validators = receiverSet.Copy() }, want: "current"},
		{name: "next", set: func(state *cmtstate.State) { state.NextValidators = receiverSet.Copy() }, want: "next"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := base()
			tc.set(&state)
			require.ErrorContains(t, requireCometStateSyncReceiverNonValidator(state, receiverKey.PubKey().Bytes()), tc.want)
		})
	}
	state := base()
	state.NextValidators = nil
	require.ErrorContains(t, requireCometStateSyncReceiverNonValidator(state, receiverKey.PubKey().Bytes()), "no next validator set")
}

type stateSyncCompletionStoreStub struct {
	height  int64
	appHash []byte
	err     error
}

func (s stateSyncCompletionStoreStub) LoadStateSyncBootstrapComplete() (int64, []byte, error) {
	return s.height, append([]byte(nil), s.appHash...), s.err
}

func TestValidateCometStateSyncHandoffRequiresExactPersistedState(t *testing.T) {
	want := sha256.Sum256([]byte("state-sync handoff"))
	state := cmtstate.State{LastBlockHeight: 42, AppHash: want[:]}

	require.NoError(t, validateCometStateSyncHandoff(state.LastBlockHeight, state.AppHash, stateSyncCompletionStoreStub{height: 42, appHash: want[:]}))
	require.ErrorIs(t, validateCometStateSyncHandoff(state.LastBlockHeight, state.AppHash, stateSyncCompletionStoreStub{}), errStateSyncBlockSyncHandoffMissing)
	require.ErrorContains(t, validateCometStateSyncHandoff(state.LastBlockHeight, state.AppHash, stateSyncCompletionStoreStub{height: 41, appHash: want[:]}), "does not match")
	wrong := sha256.Sum256([]byte("wrong handoff"))
	require.ErrorContains(t, validateCometStateSyncHandoff(state.LastBlockHeight, state.AppHash, stateSyncCompletionStoreStub{height: 42, appHash: wrong[:]}), "does not match")
	require.ErrorContains(t, validateCometStateSyncHandoff(state.LastBlockHeight, state.AppHash, stateSyncCompletionStoreStub{err: os.ErrPermission}), "permission denied")
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
		func() error { return errors.New("rollback must not complete the receiver role") },
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

func TestRecoverPendingStateSyncActivationRejectsReceiverInPersistedCometRoster(t *testing.T) {
	dataDir := t.TempDir()
	cometHome := filepath.Join(dataDir, "cometbft")
	cometCfg := config.DefaultConfig()
	cometCfg.SetRoot(cometHome)
	require.NoError(t, os.MkdirAll(filepath.Dir(cometCfg.PrivValidatorKeyFile()), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Dir(cometCfg.PrivValidatorStateFile()), 0o700))
	receiverKey := cmtcrypto.GenPrivKey()
	receiverPV := privval.NewFilePV(receiverKey, cometCfg.PrivValidatorKeyFile(), cometCfg.PrivValidatorStateFile())
	receiverPV.Save()

	oldHash := sha256.Sum256([]byte("old state"))
	newHash := sha256.Sum256([]byte("new state"))
	metadataHash := sha256.Sum256([]byte("metadata"))
	journal := statesync.ActivationJournal{
		Phase: statesync.ActivationPendingComet, Height: 42, AppHash: newHash[:], MetadataHash: metadataHash[:],
		PreparedName: "state-sync-prepared-42", QuarantineName: "badger-old-42", LiveName: "badger",
	}
	journalPath := filepath.Join(dataDir, stateSyncActivationJournalName)
	require.NoError(t, statesync.WriteActivationJournal(journalPath, journal))
	badgerPath := filepath.Join(dataDir, journal.LiveName)
	writeBootRecoveryTestState(t, badgerPath, journal.Height, newHash[:])
	writeBootRecoveryTestState(t, filepath.Join(dataDir, journal.QuarantineName), 40, oldHash[:])
	savePersistedCometStateFixtureWithKey(t, cometCfg, int64(journal.Height), newHash[:], true, receiverKey)

	completed := false
	action, found, err := recoverPendingStateSyncActivation(
		context.Background(), dataDir, cometHome, badgerPath,
		func() error {
			completed = true
			return nil
		},
	)
	require.ErrorContains(t, err, "receiver validator key is already active")
	assert.True(t, found)
	assert.Zero(t, action)
	assert.False(t, completed, "recovery must not disarm the receiver role")
	assert.FileExists(t, journalPath)
	assert.DirExists(t, badgerPath)
	assert.DirExists(t, filepath.Join(dataDir, journal.QuarantineName))
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
		func() error { return errors.New("missing journal must not complete the receiver role") },
	)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, action)
	assert.False(t, readCalled, "Comet DB stays untouched when no recovery journal exists")
}

func TestCompleteStateSyncReceivingRolePersistsOneShotDisarm(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	config := DefaultConfig(home)
	config.Quorum.StateSync.Receiving = true
	require.NoError(t, SaveConfig(config))

	require.NoError(t, completeStateSyncReceivingRole(config))
	assert.False(t, config.Quorum.StateSync.Receiving)
	require.NoError(t, completeStateSyncReceivingRole(config), "completed role is idempotent")
	written, err := os.ReadFile(filepath.Join(home, "config.yaml"))
	require.NoError(t, err)
	assert.NotContains(t, string(written), "receiving: true")
	assert.ErrorContains(t, completeStateSyncReceivingRole(nil), "required")
}

type normalServingRuntimeStub struct {
	application abcitypes.Application
	err         error
}

func (s normalServingRuntimeStub) AcquireNormalServingApplication() (abcitypes.Application, error) {
	return s.application, s.err
}

func TestAcquireNormalServingAfterStateSyncRecovery(t *testing.T) {
	application := &stateSyncRecoveryTestApp{}
	_, err := acquireNormalServingAfterStateSyncRecovery(false, normalServingRuntimeStub{application: application})
	require.ErrorContains(t, err, "recovery has not completed")
	_, err = acquireNormalServingAfterStateSyncRecovery(true, nil)
	require.ErrorContains(t, err, "runtime is required")
	_, err = acquireNormalServingAfterStateSyncRecovery(true, normalServingRuntimeStub{err: os.ErrPermission})
	require.ErrorContains(t, err, "permission denied")
	_, err = acquireNormalServingAfterStateSyncRecovery(true, normalServingRuntimeStub{})
	require.ErrorContains(t, err, "nil normal-serving application")
	got, err := acquireNormalServingAfterStateSyncRecovery(true, normalServingRuntimeStub{application: application})
	require.NoError(t, err)
	assert.Same(t, application, got)
}

type stateSyncSealRuntimeStub struct {
	phases         []sageabci.BootStateSyncPhase
	phaseCall      int
	sealCalls      int
	pending        int
	expectedHeight int64
	expectedHash   []byte
}

func (s *stateSyncSealRuntimeStub) ExpectedState() (int64, []byte) {
	return s.expectedHeight, append([]byte(nil), s.expectedHash...)
}

func (s *stateSyncSealRuntimeStub) Phase() sageabci.BootStateSyncPhase {
	index := s.phaseCall
	if index >= len(s.phases) {
		index = len(s.phases) - 1
	}
	s.phaseCall++
	return s.phases[index]
}

func (s *stateSyncSealRuntimeStub) SealActivatedBundleFromComet(
	_ context.Context,
	read func() (int64, []byte, uint64, error),
	durableSeal func(int64, []byte, uint64) error,
) (bool, int64, []byte, uint64, error) {
	s.sealCalls++
	if s.pending > 0 {
		s.pending--
		return false, 40, bytes.Repeat([]byte{0x40}, sha256.Size), 20, sageabci.ErrBootStateSyncPersistencePending
	}
	height, appHash, appVersion, err := read()
	if err == nil {
		err = durableSeal(height, appHash, appVersion)
	}
	return true, height, appHash, appVersion, err
}

func TestWaitForBootStateSyncSeal(t *testing.T) {
	hash := sha256.Sum256([]byte("persisted state"))
	runtime := &stateSyncSealRuntimeStub{
		phases:         []sageabci.BootStateSyncPhase{sageabci.BootStateSyncDiscovering, sageabci.BootStateSyncAssembling, sageabci.BootStateSyncPrepared, sageabci.BootStateSyncPendingComet},
		pending:        1,
		expectedHeight: 42,
		expectedHash:   hash[:],
	}
	catchupHash := sha256.Sum256([]byte("persisted catch-up state"))
	result, err := waitForBootStateSyncSealWithInterval(
		context.Background(), runtime,
		func(expectedHeight int64, expectedHash []byte) (int64, []byte, uint64, error) {
			require.Equal(t, int64(42), expectedHeight)
			require.Equal(t, hash[:], expectedHash)
			return 43, catchupHash[:], 20, nil
		},
		func(int64, []byte, uint64) error { return nil },
		time.Millisecond,
	)
	require.NoError(t, err)
	assert.True(t, result.activated)
	assert.Equal(t, int64(43), result.height)
	assert.Equal(t, catchupHash[:], result.appHash)
	assert.Equal(t, uint64(20), result.appVersion)
	assert.Equal(t, 2, runtime.sealCalls, "persistence lag is retried without admitting services")
}

func TestWaitForBootStateSyncSealHonorsDeadline(t *testing.T) {
	runtime := &stateSyncSealRuntimeStub{phases: []sageabci.BootStateSyncPhase{sageabci.BootStateSyncDiscovering}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, err := waitForBootStateSyncSealWithInterval(
		ctx, runtime,
		func(int64, []byte) (int64, []byte, uint64, error) { return 0, nil, 0, nil },
		func(int64, []byte, uint64) error { return nil },
		time.Millisecond,
	)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Zero(t, runtime.sealCalls)
}
