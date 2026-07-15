package node

import (
	"errors"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
)

func sageStateSyncGenesisDoc(t *testing.T, chainID string) *types.GenesisDoc {
	t.Helper()
	genesis := &types.GenesisDoc{
		GenesisTime:     time.Unix(1, 0).UTC(),
		ChainID:         chainID,
		InitialHeight:   1,
		ConsensusParams: types.DefaultConsensusParams(),
		Validators: []types.GenesisValidator{
			{PubKey: cmted25519.GenPrivKey().PubKey(), Power: 10, Name: "validator-1"},
			{PubKey: cmted25519.GenPrivKey().PubKey(), Power: 10, Name: "validator-2"},
		},
	}
	require.NoError(t, genesis.ValidateAndComplete())
	return genesis
}

func TestLoadStateFromDBOrGenesisDocProviderCachesOnlyGenesisBeforeStateSync(t *testing.T) {
	db := dbm.NewMemDB()
	genesis := sageStateSyncGenesisDoc(t, "sage-state-sync-genesis-cache")
	providerCalls := 0
	state, cached, err := LoadStateFromDBOrGenesisDocProvider(db, func() (*types.GenesisDoc, error) {
		providerCalls++
		return genesis, nil
	})
	require.NoError(t, err)
	require.Equal(t, genesis.ChainID, cached.ChainID)
	require.Zero(t, state.LastBlockHeight)
	require.False(t, state.IsEmpty(), "the genesis state exists only in memory")

	persisted, err := sm.NewStore(db, sm.StoreOptions{}).Load()
	require.NoError(t, err)
	require.True(t, persisted.IsEmpty(), "no canonical state key is persisted before state sync")
	recoverable, err := IsStateSyncGenesisDocDBResidue(db, genesis)
	require.NoError(t, err)
	require.True(t, recoverable, "the sole raw record is the exact cached genesis document")

	secondState, secondCached, err := LoadStateFromDBOrGenesisDocProvider(db, func() (*types.GenesisDoc, error) {
		providerCalls++
		return nil, errors.New("the cached genesis must bypass this provider")
	})
	require.NoError(t, err)
	require.Equal(t, genesis.ChainID, secondCached.ChainID)
	require.Zero(t, secondState.LastBlockHeight)
	require.Equal(t, 1, providerCalls)
}

func TestRecoverStateSyncGenesisDocDBResidueRemovesOnlyExactSoleRecord(t *testing.T) {
	genesis := sageStateSyncGenesisDoc(t, "sage-state-sync-genesis-recovery")
	db := dbm.NewMemDB()
	require.NoError(t, saveGenesisDoc(db, genesis))

	recoverable, err := IsStateSyncGenesisDocDBResidue(db, genesis)
	require.NoError(t, err)
	require.True(t, recoverable)
	encodedBefore, err := db.Get(genesisDocKey)
	require.NoError(t, err)
	require.NotEmpty(t, encodedBefore, "inspection never mutates the cached genesis")

	recovered, err := RecoverStateSyncGenesisDocDBResidue(db, genesis)
	require.NoError(t, err)
	require.True(t, recovered)
	encodedAfter, err := db.Get(genesisDocKey)
	require.NoError(t, err)
	require.Empty(t, encodedAfter)

	recovered, err = RecoverStateSyncGenesisDocDBResidue(db, genesis)
	require.NoError(t, err)
	require.False(t, recovered, "recovery is idempotent")
}

func TestRecoverStateSyncGenesisDocDBResiduePreservesAmbiguousData(t *testing.T) {
	genesis := sageStateSyncGenesisDoc(t, "sage-state-sync-genesis-ambiguous")
	tests := map[string]func(*testing.T, dbm.DB){
		"additional keys around genesis": func(t *testing.T, db dbm.DB) {
			require.NoError(t, saveGenesisDoc(db, genesis))
			require.NoError(t, db.SetSync([]byte("aaa"), []byte("state before genesis")))
			require.NoError(t, db.SetSync([]byte("zzz"), []byte("state after genesis")))
		},
		"different genesis": func(t *testing.T, db dbm.DB) {
			require.NoError(t, saveGenesisDoc(db, sageStateSyncGenesisDoc(t, "different-chain")))
		},
		"malformed genesis": func(t *testing.T, db dbm.DB) {
			require.NoError(t, db.SetSync(genesisDocKey, []byte("not a genesis document")))
		},
	}
	for name, populate := range tests {
		t.Run(name, func(t *testing.T) {
			db := dbm.NewMemDB()
			populate(t, db)
			before, err := db.Get(genesisDocKey)
			require.NoError(t, err)
			recoverable, err := IsStateSyncGenesisDocDBResidue(db, genesis)
			require.NoError(t, err)
			require.False(t, recoverable)
			recovered, err := RecoverStateSyncGenesisDocDBResidue(db, genesis)
			require.NoError(t, err)
			require.False(t, recovered)
			after, err := db.Get(genesisDocKey)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestStateSyncGenesisDocDBResidueRejectsInvalidInputsWithoutMutation(t *testing.T) {
	genesis := sageStateSyncGenesisDoc(t, "sage-state-sync-genesis-inputs")
	db := dbm.NewMemDB()
	require.NoError(t, saveGenesisDoc(db, genesis))
	before, err := db.Get(genesisDocKey)
	require.NoError(t, err)

	invalid := *genesis
	invalid.ChainID = ""
	_, err = IsStateSyncGenesisDocDBResidue(db, &invalid)
	require.ErrorContains(t, err, "validate expected")
	_, err = IsStateSyncGenesisDocDBResidue(db, nil)
	require.ErrorContains(t, err, "requires")
	_, err = IsStateSyncGenesisDocDBResidue(nil, genesis)
	require.ErrorContains(t, err, "requires")
	after, err := db.Get(genesisDocKey)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

type sageBootstrapStateStore struct {
	steps *[]string
	err   error
}

func (s sageBootstrapStateStore) Bootstrap(sm.State) error {
	*s.steps = append(*s.steps, "state")
	return s.err
}

type sageBootstrapCommitStore struct {
	steps     *[]string
	commitErr error
	markerErr error
}

func (s sageBootstrapCommitStore) SaveSeenCommit(int64, *types.Commit) error {
	*s.steps = append(*s.steps, "commit")
	return s.commitErr
}

func (s sageBootstrapCommitStore) SaveStateSyncBootstrapComplete(int64, []byte) error {
	*s.steps = append(*s.steps, "marker")
	return s.markerErr
}

type sageBlockSyncReactor struct {
	steps *[]string
	err   error
}

func (s sageBlockSyncReactor) SwitchToBlockSync(sm.State) error {
	*s.steps = append(*s.steps, "switch")
	return s.err
}

func TestPersistStateSyncBootstrapStoresCommitBeforePublishingState(t *testing.T) {
	steps := []string{}
	err := persistStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps},
		sm.State{LastBlockHeight: 42},
		&types.Commit{Height: 42},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"commit", "state"}, steps)
}

func TestPersistStateSyncBootstrapNeverPublishesStateAfterCommitFailure(t *testing.T) {
	steps := []string{}
	err := persistStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps, commitErr: errors.New("disk full")},
		sm.State{LastBlockHeight: 42},
		&types.Commit{Height: 42},
	)
	require.ErrorContains(t, err, "store state-sync seen commit")
	require.Equal(t, []string{"commit"}, steps)
}

func TestCompleteStateSyncBootstrapMarksOnlyAfterBlockSyncHandoff(t *testing.T) {
	steps := []string{}
	state := sm.State{LastBlockHeight: 42, AppHash: make([]byte, 32)}
	err := completeStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps},
		sageBlockSyncReactor{steps: &steps},
		state,
		&types.Commit{Height: 42},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"commit", "state", "switch", "marker"}, steps)
}

func TestCompleteStateSyncBootstrapNeverMarksAfterBlockSyncFailure(t *testing.T) {
	steps := []string{}
	state := sm.State{LastBlockHeight: 42, AppHash: make([]byte, 32)}
	err := completeStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps},
		sageBlockSyncReactor{steps: &steps, err: errors.New("reactor stopped")},
		state,
		&types.Commit{Height: 42},
	)
	require.ErrorContains(t, err, "switch to block sync")
	require.Equal(t, []string{"commit", "state", "switch"}, steps)
}

func TestCompleteStateSyncBootstrapNeverSwitchesAfterStateFailure(t *testing.T) {
	steps := []string{}
	state := sm.State{LastBlockHeight: 42, AppHash: make([]byte, 32)}
	err := completeStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps, err: errors.New("disk full")},
		sageBootstrapCommitStore{steps: &steps},
		sageBlockSyncReactor{steps: &steps},
		state,
		&types.Commit{Height: 42},
	)
	require.ErrorContains(t, err, "publish state-sync state")
	require.Equal(t, []string{"commit", "state"}, steps)
}

func TestCompleteStateSyncBootstrapReportsMarkerFailureLast(t *testing.T) {
	steps := []string{}
	state := sm.State{LastBlockHeight: 42, AppHash: make([]byte, 32)}
	err := completeStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps, markerErr: errors.New("disk full")},
		sageBlockSyncReactor{steps: &steps},
		state,
		&types.Commit{Height: 42},
	)
	require.ErrorContains(t, err, "persist state-sync block-sync handoff")
	require.Equal(t, []string{"commit", "state", "switch", "marker"}, steps)
}
