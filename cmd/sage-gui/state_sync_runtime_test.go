package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	cmtnode "github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmtstate "github.com/cometbft/cometbft/state"
	cmtstore "github.com/cometbft/cometbft/store"
	cmttypes "github.com/cometbft/cometbft/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/poe"
	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/store"
)

func newStateSyncRuntimeTestPV() *privval.FilePV {
	return privval.NewFilePV(cmted25519.GenPrivKey(), "", "")
}

func newStateSyncRuntimeTestNodeKey() *p2p.NodeKey {
	return &p2p.NodeKey{PrivKey: cmted25519.GenPrivKey()}
}

func writeStateSyncRuntimeGenesis(t *testing.T, cometCfg *config.Config, chainID string, validators ...*privval.FilePV) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(cometCfg.GenesisFile()), 0o700))
	genesisValidators := make([]cmttypes.GenesisValidator, 0, len(validators))
	for index, validator := range validators {
		publicKey, err := validator.GetPubKey()
		require.NoError(t, err)
		genesisValidators = append(genesisValidators, cmttypes.GenesisValidator{
			PubKey: publicKey,
			Power:  10,
			Name:   fmt.Sprintf("validator-%d", index+1),
		})
	}
	genesis := &cmttypes.GenesisDoc{
		GenesisTime:     time.Now().UTC(),
		ChainID:         chainID,
		InitialHeight:   1,
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators:      genesisValidators,
	}
	require.NoError(t, genesis.ValidateAndComplete())
	require.NoError(t, genesis.SaveAs(cometCfg.GenesisFile()))
}

func writeStateSyncRuntimeAuthorization(t *testing.T, path string, authorization statesync.JoinAuthorizationConfig) {
	t.Helper()
	encoded, err := json.Marshal(authorization)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
}

func cacheStateSyncRuntimeGenesis(t *testing.T, cometCfg *config.Config) {
	t.Helper()
	db, err := config.DefaultDBProvider(&config.DBContext{ID: "state", Config: cometCfg})
	require.NoError(t, err)
	state, _, loadErr := cmtnode.LoadStateFromDBOrGenesisDocProvider(db, cmtnode.DefaultGenesisDocProviderFunc(cometCfg))
	closeErr := db.Close()
	require.NoError(t, loadErr)
	require.NoError(t, closeErr)
	require.Zero(t, state.LastBlockHeight)
}

func saveStateSyncRuntimeCommitResidue(t *testing.T, cometCfg *config.Config, height int64) {
	t.Helper()
	blockDB, err := config.DefaultDBProvider(&config.DBContext{ID: "blockstore", Config: cometCfg})
	require.NoError(t, err)
	blockStore := cmtstore.NewBlockStore(blockDB)
	blockHash := sha256.Sum256([]byte("commit-only crash block"))
	partsHash := sha256.Sum256([]byte("commit-only crash parts"))
	blockID := cmttypes.BlockID{
		Hash:          blockHash[:],
		PartSetHeader: cmttypes.PartSetHeader{Total: 1, Hash: partsHash[:]},
	}
	privateKey := cmted25519.GenPrivKey()
	vote := &cmttypes.Vote{
		Type:             cmtproto.PrecommitType,
		Height:           height,
		Round:            0,
		BlockID:          blockID,
		Timestamp:        time.Unix(1, 0).UTC(),
		ValidatorAddress: privateKey.PubKey().Address(),
		ValidatorIndex:   0,
	}
	signature, err := privateKey.Sign(cmttypes.VoteSignBytes("sage-state-sync-test", vote.ToProto()))
	require.NoError(t, err)
	vote.Signature = signature
	commit := &cmttypes.Commit{
		Height:     height,
		BlockID:    blockID,
		Signatures: []cmttypes.CommitSig{vote.CommitSig()},
	}
	require.NoError(t, blockStore.SaveSeenCommit(commit.Height, commit))
	require.NoError(t, blockStore.Close())
}

func snapshotStateSyncRuntimeCometDatabase(t *testing.T, cometCfg *config.Config, database string) map[string][]byte {
	t.Helper()
	db, err := config.DefaultDBProvider(&config.DBContext{ID: database, Config: cometCfg})
	require.NoError(t, err)
	iterator, err := db.Iterator(nil, nil)
	require.NoError(t, err)
	contents := make(map[string][]byte)
	for ; iterator.Valid(); iterator.Next() {
		contents[string(iterator.Key())] = append([]byte(nil), iterator.Value()...)
	}
	require.NoError(t, iterator.Error())
	require.NoError(t, iterator.Close())
	require.NoError(t, db.Close())
	return contents
}

type stateSyncRuntimeIteratorErrorDB struct {
	dbm.DB
	err error
}

func (db stateSyncRuntimeIteratorErrorDB) Iterator(start, end []byte) (dbm.Iterator, error) {
	iterator, err := db.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return stateSyncRuntimeErrorIterator{Iterator: iterator, err: db.err}, nil
}

type stateSyncRuntimeErrorIterator struct {
	dbm.Iterator
	err error
}

func (iterator stateSyncRuntimeErrorIterator) Error() error {
	return iterator.err
}

func stateSyncRuntimePeerAddress(nodeKey *p2p.NodeKey, port int) string {
	return fmt.Sprintf("%s@127.0.0.1:%d", nodeKey.ID(), port)
}

func newStateSyncRuntimeTestStore(t *testing.T, path string, height int64, marker byte) (*store.BadgerStore, []byte) {
	t.Helper()
	require.Greater(t, height, int64(1))
	badgerStore, err := store.NewBadgerStore(path)
	require.NoError(t, err)
	require.NoError(t, badgerStore.MarkUpgradeApplied("app-v20", statesync.RequiredAppVersion, 1))
	require.NoError(t, badgerStore.SetState("appv20_legacy_resource_audit_complete_v1", []byte("complete-v1")))
	// Active app-v20 snapshots must carry the committed chain authorization
	// domain. Seed it before the trusted AppHash is calculated so the runtime
	// verifier exercises the same semantic invariant as production state sync.
	require.NoError(t, badgerStore.SetState(
		"governance_delegation_domain_v20",
		bytes.Repeat([]byte{0x5a}, sha256.Size),
	))
	require.NoError(t, badgerStore.DB().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("state-sync-test:consensus"), []byte{marker})
	}))
	appHash, err := badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state := &sageabci.AppState{
		Height:   height,
		AppHash:  append([]byte(nil), appHash...),
		EpochNum: poe.EpochNumber(height),
	}
	require.NoError(t, sageabci.SaveState(badgerStore, state))
	appHash, err = badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, sageabci.SaveState(badgerStore, state))
	return badgerStore, appHash
}

func writeStateSyncRuntimeBackup(t *testing.T, badgerStore *store.BadgerStore, path string) []byte {
	t.Helper()
	backup, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, statesync.WriteCanonicalState(context.Background(), badgerStore.DB(), backup))
	require.NoError(t, backup.Sync())
	require.NoError(t, backup.Close())
	encoded, err := os.ReadFile(path) //nolint:gosec // test-owned temporary path
	require.NoError(t, err)
	return encoded
}

func TestStateSyncRuntimePathsAndRootsFailClosed(t *testing.T) {
	dataDir := t.TempDir()
	assert.Equal(t, filepath.Join(dataDir, "state-sync", "snapshots"), resolveStateSyncPath(dataDir, "", defaultStateSyncServingDir))
	assert.Equal(t, filepath.Join(dataDir, "custom"), resolveStateSyncPath(dataDir, "custom", "ignored"))

	live := filepath.Join(dataDir, "badger")
	require.NoError(t, os.Mkdir(live, 0o700))
	require.ErrorContains(t, prepareStateSyncRoot(live, live), "overlap")
	require.ErrorContains(t, prepareStateSyncRoot(filepath.Join(live, "nested"), live), "overlap")
	require.ErrorContains(t, prepareStateSyncRoot(dataDir, live), "overlap")

	root := filepath.Join(dataDir, "network-snapshots")
	require.NoError(t, prepareStateSyncRoot(root, live))
	info, err := os.Lstat(root)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestConfigureStateSyncApplicationBundleReappliesChainPolicy(t *testing.T) {
	root := t.TempDir()
	badgerStore, err := store.NewBadgerStore(filepath.Join(root, "badger"))
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(root, "projection.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, projection.Close())
	})
	app, err := sageabci.NewSageAppWithStores(badgerStore, projection, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.CloseConsensusState())
	})

	require.NoError(t, configureStateSyncApplicationBundle(app, "v11.9-test", 77, "sage-state-sync-test"))
	info, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	assert.Equal(t, "v11.9-test", info.Version)

	err = configureStateSyncApplicationBundle(app, "ignored", 0, "")
	assert.ErrorContains(t, err, "chain_id must be non-empty")
	assert.ErrorContains(t, configureStateSyncApplicationBundle(nil, "test", 0, "sage-state-sync-test"), "expected *abci.SageApp")
}

func TestPrepareStateSyncRootRejectsSymlinkAncestorAliasOfLiveBadger(t *testing.T) {
	dataDir := t.TempDir()
	physicalDataDir := filepath.Join(dataDir, "physical")
	liveBadgerPath := filepath.Join(physicalDataDir, "badger")
	require.NoError(t, os.MkdirAll(liveBadgerPath, 0o700))

	alias := filepath.Join(dataDir, "alias")
	require.NoError(t, os.Symlink(physicalDataDir, alias))
	aliasedRoot := filepath.Join(alias, "badger", "state-sync")
	require.ErrorContains(t, prepareStateSyncRoot(aliasedRoot, liveBadgerPath), "overlap")
	_, err := os.Lstat(aliasedRoot)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestPrepareDisposableStateSyncRootOwnershipAndCleanup(t *testing.T) {
	t.Run("marker-owned leftovers are removed", func(t *testing.T) {
		dataDir := t.TempDir()
		liveBadgerPath := filepath.Join(dataDir, "badger")
		require.NoError(t, os.Mkdir(liveBadgerPath, 0o700))
		root := filepath.Join(dataDir, "receiving")
		require.NoError(t, prepareDisposableStateSyncRoot(root, liveBadgerPath))

		staleDir := filepath.Join(root, "stale", "nested")
		require.NoError(t, os.MkdirAll(staleDir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(staleDir, "chunk"), []byte("stale"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(root, "backup"), []byte("stale"), 0o600))

		require.NoError(t, prepareDisposableStateSyncRoot(root, liveBadgerPath))
		entries, err := os.ReadDir(root)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, stateSyncDisposableMarker, entries[0].Name())
	})

	t.Run("unmarked populated root is refused", func(t *testing.T) {
		dataDir := t.TempDir()
		liveBadgerPath := filepath.Join(dataDir, "badger")
		require.NoError(t, os.Mkdir(liveBadgerPath, 0o700))
		root := filepath.Join(dataDir, "operator-owned")
		require.NoError(t, os.Mkdir(root, 0o700))
		leftover := filepath.Join(root, "must-survive")
		require.NoError(t, os.WriteFile(leftover, []byte("operator data"), 0o600))

		require.ErrorContains(t, prepareDisposableStateSyncRoot(root, liveBadgerPath), "missing its secure ownership marker")
		contents, err := os.ReadFile(leftover) //nolint:gosec // test-owned temporary path
		require.NoError(t, err)
		assert.Equal(t, "operator data", string(contents))
	})
}

func TestConfigureCometStateSyncReceiver(t *testing.T) {
	comet := config.DefaultStateSyncConfig()
	cfg := validReceivingStateSyncConfig()
	tempDir := filepath.Join(t.TempDir(), "comet")
	require.NoError(t, configureCometStateSyncReceiver(comet, cfg, tempDir))
	assert.True(t, comet.Enable)
	assert.Equal(t, tempDir, comet.TempDir)
	assert.Equal(t, cfg.RPCServers, comet.RPCServers)
	assert.Equal(t, cfg.TrustHeight, comet.TrustHeight)
	assert.Equal(t, cfg.TrustHash, comet.TrustHash)
	assert.Equal(t, 168*time.Hour, comet.TrustPeriod)
	assert.Equal(t, statesync.MaxChunks, comet.MaxSnapshotChunks)
}

func TestEnsureStateSyncServingSnapshotReusesExactHeightAndRejectsConflict(t *testing.T) {
	root := filepath.Join(t.TempDir(), "snapshots")
	require.NoError(t, os.Mkdir(root, 0o700))
	source, appHash := newStateSyncRuntimeTestStore(t, filepath.Join(t.TempDir(), "source"), 5, 0x11)
	t.Cleanup(func() { _ = source.CloseBadger() })

	first, err := ensureStateSyncServingSnapshot(context.Background(), source, root, 5, appHash, statesync.MinChunkSize)
	require.NoError(t, err)
	second, err := ensureStateSyncServingSnapshot(context.Background(), source, root, 5, appHash, statesync.MinChunkSize)
	require.NoError(t, err)
	assert.Equal(t, first.Dir, second.Dir)

	conflicting, conflictingHash := newStateSyncRuntimeTestStore(t, filepath.Join(t.TempDir(), "conflicting"), 5, 0x22)
	t.Cleanup(func() { _ = conflicting.CloseBadger() })
	require.NotEqual(t, appHash, conflictingHash)
	_, err = ensureStateSyncServingSnapshot(context.Background(), conflicting, root, 5, conflictingHash, statesync.MinChunkSize)
	require.ErrorContains(t, err, "conflicting snapshot")
}

func TestStateSyncReceivePreparerVerifiesBeforeActivationAndLeavesSealJournal(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	livePath := filepath.Join(dataDir, "badger")
	live, err := store.NewBadgerStore(livePath)
	require.NoError(t, err)
	require.NoError(t, live.CloseBadger())

	source, appHash := newStateSyncRuntimeTestStore(t, filepath.Join(t.TempDir(), "source"), 7, 0x33)
	backupPath := filepath.Join(t.TempDir(), "badger.backup")
	backupBytes := writeStateSyncRuntimeBackup(t, source, backupPath)
	require.NoError(t, source.CloseBadger())
	metadata, _, _, err := statesync.BuildMetadata(7, appHash, statesync.MaxChunkSize, [][]byte{backupBytes})
	require.NoError(t, err)

	offchain, err := store.NewSQLiteStore(ctx, filepath.Join(dataDir, "projection.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = offchain.Close() })
	receiverPublicKey := cmted25519.GenPrivKey().PubKey().Bytes()
	prepare := newStateSyncReceivePreparer(dataDir, livePath, offchain, receiverPublicKey, zerolog.Nop())
	prepared, err := prepare(ctx, metadata, backupPath)
	require.NoError(t, err)
	require.NotNil(t, prepared)
	journalPath := filepath.Join(dataDir, stateSyncActivationJournalName)
	_, err = os.Lstat(journalPath)
	assert.ErrorIs(t, err, os.ErrNotExist, "isolated verification must finish before any activation journal or live rename")
	_, err = os.Lstat(livePath)
	require.NoError(t, err)

	bundle, err := prepared.Activate(ctx, nil, func() error { return nil })
	require.NoError(t, err)
	require.NotNil(t, bundle)
	height, gotHash := bundle.ExpectedState()
	assert.Equal(t, int64(7), height)
	assert.True(t, bytes.Equal(appHash, gotHash))
	assert.Equal(t, statesync.RequiredAppVersion, bundle.ExpectedAppVersion())

	journal, err := statesync.LoadActivationJournal(journalPath)
	require.NoError(t, err)
	assert.Equal(t, statesync.ActivationPendingComet, journal.Phase)
	assert.Equal(t, uint64(7), journal.Height)
	_, err = os.Lstat(filepath.Join(dataDir, journal.QuarantineName))
	require.NoError(t, err)
	require.NoError(t, statesync.SealActivatedDirectory(dataDir, journalPath, 7, appHash, 7, appHash, func() error { return nil }))
	_, err = os.Lstat(journalPath)
	assert.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, prepared.Discard())
	require.NoError(t, bundle.Close())
}

func TestStateSyncReceivePreparerRejectsAlreadyActiveReceiverValidator(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	livePath := filepath.Join(dataDir, "badger")
	live, err := store.NewBadgerStore(livePath)
	require.NoError(t, err)
	require.NoError(t, live.CloseBadger())

	receiverPublicKey := cmted25519.GenPrivKey().PubKey().Bytes()
	receiverID := fmt.Sprintf("%x", receiverPublicKey)
	source, _ := newStateSyncRuntimeTestStore(t, filepath.Join(t.TempDir(), "source"), 7, 0x34)
	require.NoError(t, source.SaveValidators(map[string]int64{receiverID: 10}))
	state, err := sageabci.LoadState(source)
	require.NoError(t, err)
	appHash, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, sageabci.SaveState(source, state))

	backupPath := filepath.Join(t.TempDir(), "badger.backup")
	backupBytes := writeStateSyncRuntimeBackup(t, source, backupPath)
	require.NoError(t, source.CloseBadger())
	metadata, _, _, err := statesync.BuildMetadata(7, appHash, statesync.MaxChunkSize, [][]byte{backupBytes})
	require.NoError(t, err)

	offchain, err := store.NewSQLiteStore(ctx, filepath.Join(dataDir, "projection.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = offchain.Close() })
	prepare := newStateSyncReceivePreparer(dataDir, livePath, offchain, receiverPublicKey, zerolog.Nop())
	_, err = prepare(ctx, metadata, backupPath)
	require.ErrorContains(t, err, "receiver validator key is already active")
	_, journalErr := os.Lstat(filepath.Join(dataDir, stateSyncActivationJournalName))
	assert.ErrorIs(t, journalErr, os.ErrNotExist)
}

func TestArmConfiguredStateSyncServingPublishesAndExposesVerifiedSnapshot(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	cometCfg := config.DefaultConfig()
	cometCfg.SetRoot(filepath.Join(dataDir, "cometbft"))
	localPV := newStateSyncRuntimeTestPV()
	otherPV := newStateSyncRuntimeTestPV()
	joiningPV := newStateSyncRuntimeTestPV()
	writeStateSyncRuntimeGenesis(t, cometCfg, "sage-state-sync-test", localPV, otherPV)
	localNode := newStateSyncRuntimeTestNodeKey()
	otherNode := newStateSyncRuntimeTestNodeKey()
	joiningNode := newStateSyncRuntimeTestNodeKey()

	badgerStore, appHash := newStateSyncRuntimeTestStore(t, filepath.Join(dataDir, "badger"), 9, 0x41)
	offchain, err := store.NewSQLiteStore(ctx, filepath.Join(dataDir, "projection.db"))
	require.NoError(t, err)
	app, err := sageabci.NewSageAppWithStores(badgerStore, offchain, zerolog.Nop())
	require.NoError(t, err)
	bundle, err := sageabci.NewConsensusBundleWithCleanup(ctx, app, app.CloseConsensusState)
	require.NoError(t, err)
	runtime, err := sageabci.NewBootStateSyncRuntime(bundle)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = runtime.Close()
		_ = offchain.Close()
	})

	joiningPublicKey, err := joiningPV.GetPubKey()
	require.NoError(t, err)
	authorizedPeerIDs := []string{string(localNode.ID()), string(otherNode.ID()), string(joiningNode.ID())}
	authorizationPath := filepath.Join(dataDir, "join.json")
	writeStateSyncRuntimeAuthorization(t, authorizationPath, statesync.JoinAuthorizationConfig{
		ChainID:             "sage-state-sync-test",
		JoiningNodeID:       string(joiningNode.ID()),
		ValidatorPublicKey:  joiningPublicKey.Bytes(),
		AppVersion:          statesync.RequiredAppVersion,
		ExpiresAt:           time.Now().Add(time.Hour),
		SnapshotHeightFloor: 5,
		ValidatorNodeIDs:    []string{string(localNode.ID()), string(otherNode.ID())},
		ProviderNodeIDs:     []string{string(localNode.ID()), string(otherNode.ID())},
	})
	cfg := &Config{
		DataDir: dataDir,
		Quorum: QuorumConfig{
			Enabled: true,
			Peers:   []string{stateSyncRuntimePeerAddress(otherNode, 26656)},
			StateSync: QuorumStateSyncConfig{
				Serving:           true,
				AuthorizationFile: authorizationPath,
				AuthorizedPeerIDs: authorizedPeerIDs,
				ChunkSize:         statesync.MinChunkSize,
			},
		},
	}
	unsafeRetention := *cfg
	unsafeRetention.RetainBlocks = 1
	require.ErrorContains(t,
		armConfiguredStateSync(ctx, &unsafeRetention, cometCfg, localPV, localNode, app, offchain, runtime, zerolog.Nop()),
		"retain_blocks=0",
	)
	require.NoError(t, armConfiguredStateSync(ctx, cfg, cometCfg, localPV, localNode, app, offchain, runtime, zerolog.Nop()))
	response, err := runtime.ListSnapshots(ctx, &abcitypes.RequestListSnapshots{})
	require.NoError(t, err)
	assert.Empty(t, response.Snapshots, "tip snapshot H must remain hidden until light blocks H+1 and H+2 exist")
	for height := int64(10); height <= 11; height++ {
		_, err = app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{
			Height: height,
			Time:   time.Unix(10_000+height, 0).UTC(),
		})
		require.NoError(t, err)
		_, err = app.Commit(ctx, &abcitypes.RequestCommit{})
		require.NoError(t, err)
	}
	response, err = runtime.ListSnapshots(ctx, &abcitypes.RequestListSnapshots{})
	require.NoError(t, err)
	require.Len(t, response.Snapshots, 1)
	assert.Equal(t, uint64(9), response.Snapshots[0].Height)
	metadata, err := statesync.DecodeMetadata(response.Snapshots[0].Metadata)
	require.NoError(t, err)
	assert.Equal(t, appHash, metadata.AppHash)
	assert.Equal(t, statesync.MaxChunks, cometCfg.StateSync.MaxSnapshotChunks)
	assert.False(t, cometCfg.StateSync.Enable)
}

func TestArmConfiguredStateSyncReceiverRetriesCometPreStateSyncCrashAndStartsOneShotPhase(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	cometCfg := config.DefaultConfig()
	cometCfg.SetRoot(filepath.Join(dataDir, "cometbft"))
	joiningPV := newStateSyncRuntimeTestPV()
	providerPV := newStateSyncRuntimeTestPV()
	writeStateSyncRuntimeGenesis(t, cometCfg, "sage-state-sync-test", providerPV)
	joiningNode := newStateSyncRuntimeTestNodeKey()
	providerOne := newStateSyncRuntimeTestNodeKey()
	providerTwo := newStateSyncRuntimeTestNodeKey()

	badgerStore, err := store.NewBadgerStore(filepath.Join(dataDir, "badger"))
	require.NoError(t, err)
	offchain, err := store.NewSQLiteStore(ctx, filepath.Join(dataDir, "projection.db"))
	require.NoError(t, err)
	app, err := sageabci.NewSageAppWithStores(badgerStore, offchain, zerolog.Nop())
	require.NoError(t, err)
	bundle, err := sageabci.NewConsensusBundleWithCleanup(ctx, app, app.CloseConsensusState)
	require.NoError(t, err)
	runtime, err := sageabci.NewBootStateSyncRuntime(bundle)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = runtime.Close()
		_ = offchain.Close()
	})

	joiningPublicKey, err := joiningPV.GetPubKey()
	require.NoError(t, err)
	authorizedPeerIDs := []string{string(providerOne.ID()), string(providerTwo.ID()), string(joiningNode.ID())}
	authorizationPath := filepath.Join(dataDir, "join.json")
	writeStateSyncRuntimeAuthorization(t, authorizationPath, statesync.JoinAuthorizationConfig{
		ChainID:             "sage-state-sync-test",
		JoiningNodeID:       string(joiningNode.ID()),
		ValidatorPublicKey:  joiningPublicKey.Bytes(),
		AppVersion:          statesync.RequiredAppVersion,
		ExpiresAt:           time.Now().Add(time.Hour),
		SnapshotHeightFloor: 5,
		ValidatorNodeIDs:    []string{string(providerOne.ID()), string(providerTwo.ID())},
		ProviderNodeIDs:     []string{string(providerOne.ID()), string(providerTwo.ID())},
	})
	stateSyncCfg := validReceivingStateSyncConfig()
	stateSyncCfg.AuthorizationFile = authorizationPath
	stateSyncCfg.AuthorizedPeerIDs = authorizedPeerIDs
	stateSyncCfg.TrustHeight = 8
	stateSyncCfg.TrustHash = strings.Repeat("cd", 32)
	cfg := &Config{
		DataDir: dataDir,
		Quorum: QuorumConfig{
			Enabled: true,
			Peers: []string{
				stateSyncRuntimePeerAddress(providerOne, 26656),
				stateSyncRuntimePeerAddress(providerTwo, 26657),
			},
			StateSync: stateSyncCfg,
		},
	}
	// Model the synchronous writes owned by the prior Comet process before its
	// asynchronous state-sync bootstrap crashed: the exact cached genesis plus
	// the commit-first record written before StateStore.Bootstrap.
	cacheStateSyncRuntimeGenesis(t, cometCfg)
	saveStateSyncRuntimeCommitResidue(t, cometCfg, 42)
	require.NoError(t, armConfiguredStateSync(ctx, cfg, cometCfg, joiningPV, joiningNode, app, offchain, runtime, zerolog.Nop()))
	assert.Equal(t, sageabci.BootStateSyncDiscovering, runtime.Phase())
	assert.True(t, cometCfg.StateSync.Enable)
	assert.Equal(t, stateSyncCfg.RPCServers, cometCfg.StateSync.RPCServers)
	assert.Equal(t, statesync.MaxChunks, cometCfg.StateSync.MaxSnapshotChunks)
	assert.Equal(t, filepath.Join(dataDir, defaultStateSyncReceivingDir, stateSyncCometTempDirname), cometCfg.StateSync.TempDir)
	require.NoError(t, requireEmptyCometDatabase(cometCfg, "state"))
	require.NoError(t, requireEmptyCometDatabase(cometCfg, "blockstore"))
}

func TestFreshStateSyncReceiverRejectsSoloLocalGenesis(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	cometCfg := config.DefaultConfig()
	cometCfg.SetRoot(filepath.Join(dataDir, "cometbft"))
	localPV := newStateSyncRuntimeTestPV()
	writeStateSyncRuntimeGenesis(t, cometCfg, "sage-state-sync-test", localPV)
	badgerStore, err := store.NewBadgerStore(filepath.Join(dataDir, "badger"))
	require.NoError(t, err)
	offchain, err := store.NewSQLiteStore(ctx, filepath.Join(dataDir, "projection.db"))
	require.NoError(t, err)
	app, err := sageabci.NewSageAppWithStores(badgerStore, offchain, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = app.Close() })

	err = requireFreshStateSyncReceiver(ctx, cometCfg, localPV, app, offchain)
	require.ErrorContains(t, err, "only the local validator")
}

func TestFreshStateSyncReceiverRecoversExactCometPreStateSyncResidues(t *testing.T) {
	tests := []struct {
		name            string
		cachedGenesis   bool
		commitBootstrap bool
	}{
		{name: "cached genesis only", cachedGenesis: true},
		{name: "cached genesis plus commit-only bootstrap", cachedGenesis: true, commitBootstrap: true},
		{name: "commit-only after cached-genesis cleanup", commitBootstrap: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cometCfg, pv, app, offchain := newFreshStateSyncReceiverFixture(t)
			if test.cachedGenesis {
				cacheStateSyncRuntimeGenesis(t, cometCfg)
			}
			if test.commitBootstrap {
				saveStateSyncRuntimeCommitResidue(t, cometCfg, 42)
			}
			if test.cachedGenesis {
				require.NotEmpty(t, snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "state"))
			}
			if test.commitBootstrap {
				require.NotEmpty(t, snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "blockstore"))
			}

			require.NoError(t, requireFreshStateSyncReceiver(ctx, cometCfg, pv, app, offchain))
			require.NoError(t, requireEmptyCometDatabase(cometCfg, "state"))
			require.NoError(t, requireEmptyCometDatabase(cometCfg, "blockstore"))
			require.NoError(t, requireFreshStateSyncReceiver(ctx, cometCfg, pv, app, offchain), "cleanup remains idempotently fresh")
		})
	}
}

func TestFreshStateSyncReceiverRejectsAmbiguousCometResiduesWithoutMutation(t *testing.T) {
	tests := map[string]func(*testing.T, *config.Config){
		"additional state key": func(t *testing.T, cometCfg *config.Config) {
			db, err := config.DefaultDBProvider(&config.DBContext{ID: "state", Config: cometCfg})
			require.NoError(t, err)
			require.NoError(t, db.SetSync([]byte("unexpected-state"), []byte("preserve me")))
			require.NoError(t, db.Close())
		},
		"configured genesis changed": func(t *testing.T, cometCfg *config.Config) {
			genesis, err := cmttypes.GenesisDocFromFile(cometCfg.GenesisFile())
			require.NoError(t, err)
			genesis.ChainID += "-changed"
			require.NoError(t, genesis.SaveAs(cometCfg.GenesisFile()))
		},
		"malformed cached genesis": func(t *testing.T, cometCfg *config.Config) {
			db, err := config.DefaultDBProvider(&config.DBContext{ID: "state", Config: cometCfg})
			require.NoError(t, err)
			require.NoError(t, db.SetSync([]byte("genesisDoc"), []byte("malformed cached genesis")))
			require.NoError(t, db.Close())
		},
		"additional blockstore key": func(t *testing.T, cometCfg *config.Config) {
			db, err := config.DefaultDBProvider(&config.DBContext{ID: "blockstore", Config: cometCfg})
			require.NoError(t, err)
			require.NoError(t, db.SetSync([]byte("unexpected-block"), []byte("preserve me")))
			require.NoError(t, db.Close())
		},
		"malformed commit residue": func(t *testing.T, cometCfg *config.Config) {
			db, err := config.DefaultDBProvider(&config.DBContext{ID: "blockstore", Config: cometCfg})
			require.NoError(t, err)
			require.NoError(t, db.SetSync([]byte("SC:42"), []byte("malformed commit")))
			require.NoError(t, db.Close())
		},
		"canonical all-absent commit residue": func(t *testing.T, cometCfg *config.Config) {
			db, err := config.DefaultDBProvider(&config.DBContext{ID: "blockstore", Config: cometCfg})
			require.NoError(t, err)
			blockStore := cmtstore.NewBlockStore(db)
			commit := blockStore.LoadSeenCommit(42)
			require.NotNil(t, commit)
			commit.Signatures = []cmttypes.CommitSig{cmttypes.NewCommitSigAbsent()}
			require.NoError(t, blockStore.SaveSeenCommit(42, commit))
			require.NoError(t, blockStore.Close())
		},
		"canonical short commit signature": func(t *testing.T, cometCfg *config.Config) {
			db, err := config.DefaultDBProvider(&config.DBContext{ID: "blockstore", Config: cometCfg})
			require.NoError(t, err)
			blockStore := cmtstore.NewBlockStore(db)
			commit := blockStore.LoadSeenCommit(42)
			require.NotNil(t, commit)
			commit.Signatures[0].Signature = []byte{0x01}
			require.NoError(t, blockStore.SaveSeenCommit(42, commit))
			require.NoError(t, blockStore.Close())
		},
		"persisted canonical state": func(t *testing.T, cometCfg *config.Config) {
			genesis, err := cmttypes.GenesisDocFromFile(cometCfg.GenesisFile())
			require.NoError(t, err)
			genesisState, err := cmtstate.MakeGenesisState(genesis)
			require.NoError(t, err)
			db, err := config.DefaultDBProvider(&config.DBContext{ID: "state", Config: cometCfg})
			require.NoError(t, err)
			require.NoError(t, cmtstate.NewStore(db, cmtstate.StoreOptions{}).Save(genesisState))
			require.NoError(t, db.Close())
		},
		"malformed canonical state key": func(t *testing.T, cometCfg *config.Config) {
			db, err := config.DefaultDBProvider(&config.DBContext{ID: "state", Config: cometCfg})
			require.NoError(t, err)
			require.NoError(t, db.SetSync([]byte("stateKey"), []byte("malformed state protobuf")))
			require.NoError(t, db.Close())
		},
		"populated evidence database": func(t *testing.T, cometCfg *config.Config) {
			populateStateSyncRuntimeCometDatabase(t, cometCfg, "evidence")
		},
		"populated transaction index": func(t *testing.T, cometCfg *config.Config) {
			populateStateSyncRuntimeCometDatabase(t, cometCfg, "tx_index")
		},
		"nonempty consensus WAL": func(t *testing.T, cometCfg *config.Config) {
			walPath := cometCfg.Consensus.WalFile()
			require.NoError(t, os.MkdirAll(filepath.Dir(walPath), 0o700))
			require.NoError(t, os.WriteFile(walPath, []byte("preserve signing evidence"), 0o600))
		},
	}

	for name, contaminate := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cometCfg, pv, app, offchain := newFreshStateSyncReceiverFixture(t)
			cacheStateSyncRuntimeGenesis(t, cometCfg)
			saveStateSyncRuntimeCommitResidue(t, cometCfg, 42)
			contaminate(t, cometCfg)
			stateBefore := snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "state")
			blockBefore := snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "blockstore")

			err := requireFreshStateSyncReceiver(ctx, cometCfg, pv, app, offchain)
			require.Error(t, err)
			require.Equal(t, stateBefore, snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "state"))
			require.Equal(t, blockBefore, snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "blockstore"))
		})
	}

	t.Run("nonzero validator signing state", func(t *testing.T) {
		ctx, cometCfg, pv, app, offchain := newFreshStateSyncReceiverFixture(t)
		cacheStateSyncRuntimeGenesis(t, cometCfg)
		saveStateSyncRuntimeCommitResidue(t, cometCfg, 42)
		pv.LastSignState.Height = 1
		stateBefore := snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "state")
		blockBefore := snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "blockstore")

		err := requireFreshStateSyncReceiver(ctx, cometCfg, pv, app, offchain)
		require.ErrorContains(t, err, "fresh validator signing state")
		require.Equal(t, stateBefore, snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "state"))
		require.Equal(t, blockBefore, snapshotStateSyncRuntimeCometDatabase(t, cometCfg, "blockstore"))
	})
}

func TestRequireEmptyCometDatabaseFailsClosedOnIteratorReadError(t *testing.T) {
	cometCfg := config.DefaultConfig()
	db := stateSyncRuntimeIteratorErrorDB{DB: dbm.NewMemDB(), err: fmt.Errorf("injected iterator read failure")}
	err := requireEmptyCometDatabaseWithProvider(cometCfg, "state", func(*config.DBContext) (dbm.DB, error) {
		return db, nil
	})
	require.ErrorContains(t, err, "injected iterator read failure")
}

func TestFreshStateSyncReceiverRejectsMalformedBlockStoreMetadataWithoutPanic(t *testing.T) {
	ctx, cometCfg, pv, app, offchain := newFreshStateSyncReceiverFixture(t)
	blockDB, err := config.DefaultDBProvider(&config.DBContext{ID: "blockstore", Config: cometCfg})
	require.NoError(t, err)
	require.NoError(t, blockDB.SetSync([]byte("blockStore"), []byte("malformed block-store metadata")))
	require.NoError(t, blockDB.Close())

	err = requireFreshStateSyncReceiver(ctx, cometCfg, pv, app, offchain)
	require.ErrorContains(t, err, "empty CometBFT blockstore database")
}

func TestStateSyncReceiverPreservesSigningAndWALFreshnessEvidence(t *testing.T) {
	ctx, cometCfg, _, app, offchain := newFreshStateSyncReceiverFixture(t)
	require.NoError(t, os.MkdirAll(filepath.Dir(cometCfg.PrivValidatorKeyFile()), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Dir(cometCfg.PrivValidatorStateFile()), 0o700))
	pv := privval.GenFilePV(cometCfg.PrivValidatorKeyFile(), cometCfg.PrivValidatorStateFile())
	pv.LastSignState.Height = 7
	pv.Save()
	pv = privval.LoadFilePV(cometCfg.PrivValidatorKeyFile(), cometCfg.PrivValidatorStateFile())

	walPath := cometCfg.Consensus.WalFile()
	require.NoError(t, os.MkdirAll(filepath.Dir(walPath), 0o700))
	require.NoError(t, os.WriteFile(walPath, []byte("signed receiver WAL evidence"), 0o600))
	stateBefore, err := os.ReadFile(cometCfg.PrivValidatorStateFile())
	require.NoError(t, err)
	walBefore, err := os.ReadFile(walPath)
	require.NoError(t, err)

	preserved, err := repairMissingBlockStoreArtifacts(cometCfg, pv, true, zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, int64(7), preserved.LastSignState.Height)
	stateAfter, err := os.ReadFile(cometCfg.PrivValidatorStateFile())
	require.NoError(t, err)
	walAfter, err := os.ReadFile(walPath)
	require.NoError(t, err)
	require.Equal(t, stateBefore, stateAfter)
	require.Equal(t, walBefore, walAfter)
	require.ErrorContains(t, requireFreshStateSyncReceiver(ctx, cometCfg, preserved, app, offchain), "absent or empty consensus WAL")

	require.NoError(t, os.Remove(walPath))
	require.ErrorContains(t, requireFreshStateSyncReceiver(ctx, cometCfg, preserved, app, offchain), "fresh validator signing state")
	stateAfterRejection, err := os.ReadFile(cometCfg.PrivValidatorStateFile())
	require.NoError(t, err)
	require.Equal(t, stateBefore, stateAfterRejection)
}

func TestFreshStateSyncReceiverRejectsEveryPersistedSurface(t *testing.T) {
	tests := []struct {
		name        string
		error       string
		contaminate func(*testing.T, context.Context, *config.Config, *privval.FilePV, *sageabci.SageApp, *store.SQLiteStore) *privval.FilePV
	}{
		{
			name:  "nonempty raw Badger at height zero",
			error: "empty Badger keyspace",
			contaminate: func(t *testing.T, ctx context.Context, _ *config.Config, pv *privval.FilePV, app *sageabci.SageApp, _ *store.SQLiteStore) *privval.FilePV {
				require.NoError(t, app.GetBadgerStore().DB().Update(func(txn *badger.Txn) error {
					return txn.Set([]byte("raw-stale-key"), []byte("raw-stale-value"))
				}))
				info, err := app.Info(ctx, nil)
				require.NoError(t, err)
				require.Zero(t, info.LastBlockHeight)
				require.Empty(t, info.LastBlockAppHash)
				return pv
			},
		},
		{
			name:  "populated SQLite projection",
			error: "empty off-chain projection",
			contaminate: func(t *testing.T, ctx context.Context, _ *config.Config, pv *privval.FilePV, _ *sageabci.SageApp, offchain *store.SQLiteStore) *privval.FilePV {
				require.NoError(t, offchain.SetPreference(ctx, "state-sync-test", "stale"))
				return pv
			},
		},
		{
			name:  "populated CometBFT blockstore",
			error: "empty CometBFT blockstore database",
			contaminate: func(t *testing.T, _ context.Context, cometCfg *config.Config, pv *privval.FilePV, _ *sageabci.SageApp, _ *store.SQLiteStore) *privval.FilePV {
				populateStateSyncRuntimeCometDatabase(t, cometCfg, "blockstore")
				return pv
			},
		},
		{
			name:  "populated CometBFT evidence",
			error: "empty CometBFT evidence database",
			contaminate: func(t *testing.T, _ context.Context, cometCfg *config.Config, pv *privval.FilePV, _ *sageabci.SageApp, _ *store.SQLiteStore) *privval.FilePV {
				populateStateSyncRuntimeCometDatabase(t, cometCfg, "evidence")
				return pv
			},
		},
		{
			name:  "populated CometBFT tx index",
			error: "empty CometBFT tx_index database",
			contaminate: func(t *testing.T, _ context.Context, cometCfg *config.Config, pv *privval.FilePV, _ *sageabci.SageApp, _ *store.SQLiteStore) *privval.FilePV {
				populateStateSyncRuntimeCometDatabase(t, cometCfg, "tx_index")
				return pv
			},
		},
		{
			name:  "nonempty consensus WAL",
			error: "absent or empty consensus WAL",
			contaminate: func(t *testing.T, _ context.Context, cometCfg *config.Config, pv *privval.FilePV, _ *sageabci.SageApp, _ *store.SQLiteStore) *privval.FilePV {
				walPath := cometCfg.Consensus.WalFile()
				require.NoError(t, os.MkdirAll(filepath.Dir(walPath), 0o700))
				require.NoError(t, os.WriteFile(walPath, []byte("stale WAL"), 0o600))
				return pv
			},
		},
		{
			name:  "nonzero FilePV signing state",
			error: "fresh validator signing state",
			contaminate: func(t *testing.T, _ context.Context, _ *config.Config, _ *privval.FilePV, _ *sageabci.SageApp, _ *store.SQLiteStore) *privval.FilePV {
				privValDir := t.TempDir()
				keyPath := filepath.Join(privValDir, "priv_validator_key.json")
				statePath := filepath.Join(privValDir, "priv_validator_state.json")
				persisted := privval.GenFilePV(keyPath, statePath)
				persisted.LastSignState.Height = 1
				persisted.Save()
				return privval.LoadFilePV(keyPath, statePath)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cometCfg, pv, app, offchain := newFreshStateSyncReceiverFixture(t)
			pv = test.contaminate(t, ctx, cometCfg, pv, app, offchain)
			require.ErrorContains(t, requireFreshStateSyncReceiver(ctx, cometCfg, pv, app, offchain), test.error)
		})
	}
}

func newFreshStateSyncReceiverFixture(t *testing.T) (context.Context, *config.Config, *privval.FilePV, *sageabci.SageApp, *store.SQLiteStore) {
	t.Helper()
	ctx := context.Background()
	dataDir := t.TempDir()
	cometCfg := config.DefaultConfig()
	cometCfg.SetRoot(filepath.Join(dataDir, "cometbft"))
	localPV := newStateSyncRuntimeTestPV()
	writeStateSyncRuntimeGenesis(t, cometCfg, "sage-state-sync-test", localPV, newStateSyncRuntimeTestPV())
	badgerStore, err := store.NewBadgerStore(filepath.Join(dataDir, "badger"))
	require.NoError(t, err)
	offchain, err := store.NewSQLiteStore(ctx, filepath.Join(dataDir, "projection.db"))
	require.NoError(t, err)
	app, err := sageabci.NewSageAppWithStores(badgerStore, offchain, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = app.Close() })
	return ctx, cometCfg, localPV, app, offchain
}

func populateStateSyncRuntimeCometDatabase(t *testing.T, cometCfg *config.Config, database string) {
	t.Helper()
	db, err := config.DefaultDBProvider(&config.DBContext{ID: database, Config: cometCfg})
	require.NoError(t, err)
	require.NoError(t, db.SetSync([]byte("stale-key"), []byte("stale-value")))
	require.NoError(t, db.Close())
}
