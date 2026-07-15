package abci

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/scope"
	sagesnapshot "github.com/l33tdawg/sage/internal/snapshot"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

func finalizeAndCommitV20Block(t *testing.T, app *SageApp, req *abcitypes.RequestFinalizeBlock) *abcitypes.ResponseFinalizeBlock {
	t.Helper()
	resp, err := app.FinalizeBlock(context.Background(), req)
	require.NoError(t, err)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	return resp
}

func signedV20Tx(t *testing.T, parsed *tx.ParsedTx, signer agentKey) []byte {
	t.Helper()
	require.NoError(t, tx.SignTx(parsed, signer.priv))
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return raw
}

// TestAppV20OfflineReplicaCatchesUpByOrderedABCIReplay models a validator that
// misses several committed blocks, then receives those exact blocks in order.
// This is deliberately an ABCI replay proof, not a claim that CometBFT network
// state sync is implemented. The latter needs a separate public snapshot format.
func TestAppV20OfflineReplicaCatchesUpByOrderedABCIReplay(t *testing.T) {
	validators := []agentKey{
		deterministicScopedAgent(1),
		deterministicScopedAgent(33),
		deterministicScopedAgent(65),
	}
	sort.Slice(validators, func(i, j int) bool { return validators[i].id < validators[j].id })

	buildReplica := func() *SageApp {
		app := setupTestApp(t)
		app.appV20AppliedHeight = 1
		for _, key := range validators {
			require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
				ID: key.id, PublicKey: key.pub, Power: 10,
			}))
		}
		for _, domain := range []string{"research", "private-notes"} {
			require.NoError(t, app.badgerStore.RegisterDomain(domain, validators[0].id, "", 1))
			require.NoError(t, app.badgerStore.SetAccessGrant(domain, validators[0].id, 2, 0, validators[0].id))
		}
		installScopeForValidators(t, app, "scope-replay", "research", 1, scope.StateActive, validators)
		return app
	}

	leader := buildReplica()
	offline := buildReplica()
	type committedBlock struct {
		req     *abcitypes.RequestFinalizeBlock
		appHash []byte
		txCode  uint32
		txData  []byte
	}
	blocks := make([]committedBlock, 0, 5)
	appendLeaderBlock := func(height int64, raw []byte) *abcitypes.ResponseFinalizeBlock {
		req := &abcitypes.RequestFinalizeBlock{
			Height: height,
			Time:   time.Unix(1_000+height, 0).UTC(),
			Txs:    [][]byte{raw},
		}
		resp := finalizeAndCommitV20Block(t, leader, req)
		require.Len(t, resp.TxResults, 1)
		require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
		blocks = append(blocks, committedBlock{
			req: req, appHash: append([]byte(nil), resp.AppHash...),
			txCode: resp.TxResults[0].Code, txData: append([]byte(nil), resp.TxResults[0].Data...),
		})
		return resp
	}

	scopedSubmit := makeMemorySubmitTx(t, validators[0], "research", "selected canonical knowledge")
	scopedSubmit.Nonce = 1
	scopedResp := appendLeaderBlock(2, signedV20Tx(t, scopedSubmit, validators[0]))
	scopedMemoryID := string(scopedResp.TxResults[0].Data)
	require.NotEmpty(t, scopedMemoryID)

	unselectedSubmit := makeMemorySubmitTx(t, validators[0], "private-notes", "local content outside the selected scope")
	unselectedSubmit.Nonce = 2
	unselectedResp := appendLeaderBlock(3, signedV20Tx(t, unselectedSubmit, validators[0]))
	unselectedMemoryID := string(unselectedResp.TxResults[0].Data)
	require.NotEmpty(t, unselectedMemoryID)

	for i, key := range validators {
		nonce := uint64(1)
		if i == 0 {
			nonce = 3
		}
		vote := &tx.ParsedTx{
			Type:  tx.TxTypeMemoryVote,
			Nonce: nonce,
			MemoryVote: &tx.MemoryVote{
				MemoryID: scopedMemoryID,
				Decision: tx.VoteDecisionAccept,
			},
		}
		appendLeaderBlock(int64(4+i), signedV20Tx(t, vote, key))
	}

	// The second validator now catches up solely by replaying the exact ordered
	// block inputs. Compare every intermediate AppHash, not merely the final one.
	for _, block := range blocks {
		resp := finalizeAndCommitV20Block(t, offline, block.req)
		require.Len(t, resp.TxResults, 1)
		assert.Equal(t, block.txCode, resp.TxResults[0].Code, "tx result diverged at height %d", block.req.Height)
		assert.Equal(t, block.txData, resp.TxResults[0].Data, "tx data diverged at height %d", block.req.Height)
		assert.Equal(t, block.appHash, resp.AppHash, "AppHash diverged at height %d", block.req.Height)
	}

	leaderBallot, err := leader.badgerStore.GetScopeBallot(scopedMemoryID)
	require.NoError(t, err)
	offlineBallot, err := offline.badgerStore.GetScopeBallot(scopedMemoryID)
	require.NoError(t, err)
	assert.Equal(t, leaderBallot, offlineBallot)
	require.NotNil(t, offlineBallot)
	assert.Equal(t, scope.BallotCommitted, offlineBallot.State)

	leaderContent, err := leader.badgerStore.ListScopedContents()
	require.NoError(t, err)
	offlineContent, err := offline.badgerStore.ListScopedContents()
	require.NoError(t, err)
	assert.Equal(t, leaderContent, offlineContent)
	require.Len(t, offlineContent, 1, "only selected domains receive canonical recoverable envelopes")
	assert.Equal(t, scopedMemoryID, offlineContent[0].MemoryID)
	notSelected, err := offline.badgerStore.GetScopedContent(unselectedMemoryID)
	require.NoError(t, err)
	assert.Nil(t, notSelected, "an unselected domain must not leak into scoped recovery state")
}

// TestAppV20LocalSnapshotRestoresCanonicalScopeAndRebuildsLostProjection
// exercises the existing operator rollback bundle end to end. It intentionally
// deletes the restored SQLite database before boot to prove that AppHash-covered
// scoped content in Badger is sufficient to reconstruct the local read model.
func TestAppV20LocalSnapshotRestoresCanonicalScopeAndRebuildsLostProjection(t *testing.T) {
	root := t.TempDir()
	sourceData := filepath.Join(root, "source", "data")
	restoredData := filepath.Join(root, "restored", "data")
	require.NoError(t, os.MkdirAll(filepath.Join(sourceData, "badger"), 0o700))
	seedV20SnapshotFilesystem(t, sourceData)

	badgerStore, err := store.NewBadgerStore(filepath.Join(sourceData, "badger"))
	require.NoError(t, err)
	require.NoError(t, badgerStore.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	sqliteStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(sourceData, "sage.db"))
	require.NoError(t, err)
	app, err := NewSageAppWithStores(badgerStore, sqliteStore, zerolog.Nop())
	require.NoError(t, err)
	sourceClosed := false
	t.Cleanup(func() {
		if !sourceClosed {
			_ = app.Close()
		}
	})

	validators := []agentKey{
		deterministicScopedAgent(1),
		deterministicScopedAgent(33),
		deterministicScopedAgent(65),
	}
	sort.Slice(validators, func(i, j int) bool { return validators[i].id < validators[j].id })
	persistedValidators := make(map[string]int64, len(validators))
	for _, key := range validators {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID: key.id, PublicKey: key.pub, Power: 10,
		}))
		persistedValidators[key.id] = 10
	}
	require.NoError(t, badgerStore.SaveValidators(persistedValidators))
	require.NoError(t, badgerStore.RegisterDomain("research", validators[0].id, "", 1))
	require.NoError(t, badgerStore.SetAccessGrant("research", validators[0].id, 2, 0, validators[0].id))
	installScopeForValidators(t, app, "scope-snapshot", "research", 1, scope.StateActive, validators)

	submit := makeMemorySubmitTx(t, validators[0], "research", "survives snapshot without its SQL projection")
	submit.Nonce = 1
	submitResp := finalizeAndCommitV20Block(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(2_000, 0).UTC(), Txs: [][]byte{signedV20Tx(t, submit, validators[0])},
	})
	require.Zero(t, submitResp.TxResults[0].Code, submitResp.TxResults[0].Log)
	memoryID := string(submitResp.TxResults[0].Data)
	for i, key := range validators {
		nonce := uint64(1)
		if i == 0 {
			nonce = 2
		}
		vote := &tx.ParsedTx{
			Type:  tx.TxTypeMemoryVote,
			Nonce: nonce,
			MemoryVote: &tx.MemoryVote{
				MemoryID: memoryID,
				Decision: tx.VoteDecisionAccept,
			},
		}
		resp := finalizeAndCommitV20Block(t, app, &abcitypes.RequestFinalizeBlock{
			Height: int64(3 + i), Time: time.Unix(int64(2_003+i), 0).UTC(),
			Txs: [][]byte{signedV20Tx(t, vote, key)},
		})
		require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
	}

	appHash, err := badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	const snapshotHeight = int64(5)
	manifest, err := sagesnapshot.Take(context.Background(), sourceData, snapshotHeight, appHash, "app-v20-scoped-recovery", sagesnapshot.Options{
		BinaryVersion: "v11.9-test",
		IncludeBinary: false,
		LiveBadger:    badgerStore.DB(),
	})
	require.NoError(t, err)
	assert.Equal(t, appHash, manifest.AppHash)
	snapshotDir := filepath.Join(sourceData, "snapshots", "5")
	require.NoError(t, sagesnapshot.Verify(snapshotDir))
	require.NoError(t, app.Close())
	sourceClosed = true

	restoredHeight, err := sagesnapshot.Restore(snapshotDir, restoredData)
	require.NoError(t, err)
	assert.Equal(t, snapshotHeight, restoredHeight)
	// Simulate total loss of the local query projection. Canonical recovery
	// must need only the restored consensus database.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		removeErr := os.Remove(filepath.Join(restoredData, "sage.db") + suffix)
		require.True(t, removeErr == nil || os.IsNotExist(removeErr), "remove discarded projection%s: %v", suffix, removeErr)
	}

	restoredBadger, err := store.NewBadgerStore(filepath.Join(restoredData, "badger"))
	require.NoError(t, err)
	restoredSQLite, err := store.NewSQLiteStore(context.Background(), filepath.Join(restoredData, "sage.db"))
	require.NoError(t, err)
	restoredApp, err := NewSageAppWithStores(restoredBadger, restoredSQLite, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = restoredApp.Close() })

	restoredHash, err := restoredBadger.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	assert.Equal(t, manifest.AppHash, restoredHash)
	assert.True(t, restoredApp.IsAppV20ActiveForNextTx(), "the applied app-v20 audit must survive restore")
	rebuilt, err := restoredApp.RebuildScopedProjection(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, rebuilt)
	rebuilt, err = restoredApp.RebuildScopedProjection(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, rebuilt, "projection rebuild remains idempotent after snapshot restore")

	projected, err := restoredSQLite.GetMemory(context.Background(), memoryID)
	require.NoError(t, err)
	assert.Equal(t, submit.MemorySubmit.Content, projected.Content)
	assert.Equal(t, submit.MemorySubmit.ContentHash, projected.ContentHash)
	assert.Equal(t, "research", projected.DomainTag)
	assert.Equal(t, validators[0].id, projected.SubmittingAgent)
	assert.Equal(t, memory.StatusCommitted, projected.Status)
	classification, err := restoredSQLite.GetMemoryClassificationLocal(context.Background(), memoryID)
	require.NoError(t, err)
	assert.Equal(t, int(submit.MemorySubmit.Classification), classification)

	restoredContents, err := restoredBadger.ListScopedContents()
	require.NoError(t, err)
	require.Len(t, restoredContents, 1)
	assert.Equal(t, memoryID, restoredContents[0].MemoryID)
	restoredBallot, err := restoredBadger.GetScopeBallot(memoryID)
	require.NoError(t, err)
	require.NotNil(t, restoredBallot)
	assert.Equal(t, scope.BallotCommitted, restoredBallot.State)
}

// TestAppV20ScopedCrashBetweenFinalizeAndCommitReplaysProjection proves the
// crash window called out by Commit's durability contract. Badger already
// contains the scoped consensus effects and consumed nonce, while persisted
// state.Height and SQLite remain behind. The exact block replay must recover;
// the same signed transaction at any later height must remain rejected.
func TestAppV20ScopedCrashBetweenFinalizeAndCommitReplaysProjection(t *testing.T) {
	root := t.TempDir()
	badgerPath := filepath.Join(root, "badger")
	sqlitePath := filepath.Join(root, "sage.db")
	require.NoError(t, os.MkdirAll(badgerPath, 0o700))

	badgerStore, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	require.NoError(t, badgerStore.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	sqliteStore, err := store.NewSQLiteStore(context.Background(), sqlitePath)
	require.NoError(t, err)
	app, err := NewSageAppWithStores(badgerStore, sqliteStore, zerolog.Nop())
	require.NoError(t, err)
	currentApp := app
	t.Cleanup(func() {
		if currentApp != nil {
			_ = currentApp.Close()
		}
	})

	validators := []agentKey{
		deterministicScopedAgent(1),
		deterministicScopedAgent(33),
		deterministicScopedAgent(65),
	}
	sort.Slice(validators, func(i, j int) bool { return validators[i].id < validators[j].id })
	persistedValidators := make(map[string]int64, len(validators))
	for _, key := range validators {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID: key.id, PublicKey: key.pub, Power: 10,
		}))
		persistedValidators[key.id] = 10
	}
	require.NoError(t, badgerStore.SaveValidators(persistedValidators))
	require.NoError(t, badgerStore.RegisterDomain("research", validators[0].id, "", 1))
	require.NoError(t, badgerStore.SetAccessGrant("research", validators[0].id, 2, 0, validators[0].id))
	installScopeForValidators(t, app, "scope-crash", "research", 1, scope.StateActive, validators)

	submit := makeMemorySubmitTx(t, validators[0], "research", "finalized before projection crash")
	submit.Nonce = 1
	rawSubmit := signedV20Tx(t, submit, validators[0])
	submitBlock := &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(3_002, 0).UTC(), Txs: [][]byte{rawSubmit},
	}
	// Inject a permanent SQL failure only after construction. FinalizeBlock
	// writes canonical Badger state; Commit must panic before SaveState.
	app.offchainStore = &busyInjectingStore{OffchainStore: sqliteStore, alwaysFail: true}
	app.flushMaxRetries = 1
	firstSubmitResp, err := app.FinalizeBlock(context.Background(), submitBlock)
	require.NoError(t, err)
	require.Zero(t, firstSubmitResp.TxResults[0].Code, firstSubmitResp.TxResults[0].Log)
	memoryID := string(firstSubmitResp.TxResults[0].Data)
	requireCommitPanicV20(t, app)
	require.NoError(t, app.Close())
	currentApp = nil

	openRecoveredApp := func() (*SageApp, *store.SQLiteStore) {
		t.Helper()
		bs, openErr := store.NewBadgerStore(badgerPath)
		require.NoError(t, openErr)
		projection, openErr := store.NewSQLiteStore(context.Background(), sqlitePath)
		require.NoError(t, openErr)
		recovered, openErr := NewSageAppWithStores(bs, projection, zerolog.Nop())
		require.NoError(t, openErr)
		return recovered, projection
	}

	app, sqliteStore = openRecoveredApp()
	currentApp = app
	assert.Equal(t, int64(0), app.state.Height, "failed Commit must leave persisted height behind")
	rebuilt, err := app.RebuildScopedProjection(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, rebuilt)
	replayedSubmitResp := finalizeAndCommitV20Block(t, app, submitBlock)
	require.Zero(t, replayedSubmitResp.TxResults[0].Code, replayedSubmitResp.TxResults[0].Log)
	assert.Equal(t, firstSubmitResp.TxResults[0].Data, replayedSubmitResp.TxResults[0].Data)
	assert.Equal(t, firstSubmitResp.AppHash, replayedSubmitResp.AppHash)
	projected, err := sqliteStore.GetMemory(context.Background(), memoryID)
	require.NoError(t, err)
	assert.Equal(t, memory.StatusProposed, projected.Status)

	// Two votes commit normally. The quorum-reaching third vote then crashes in
	// the same FinalizeBlock→Commit gap and must recover from its immutable
	// decision+height witness without re-crediting validator statistics.
	for i := 0; i < 2; i++ {
		nonce := uint64(1)
		if i == 0 {
			nonce = 2
		}
		vote := &tx.ParsedTx{Type: tx.TxTypeMemoryVote, Nonce: nonce, MemoryVote: &tx.MemoryVote{
			MemoryID: memoryID, Decision: tx.VoteDecisionAccept,
		}}
		resp := finalizeAndCommitV20Block(t, app, &abcitypes.RequestFinalizeBlock{
			Height: int64(3 + i), Time: time.Unix(int64(3_003+i), 0).UTC(),
			Txs: [][]byte{signedV20Tx(t, vote, validators[i])},
		})
		require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
	}
	finalVote := &tx.ParsedTx{Type: tx.TxTypeMemoryVote, Nonce: 1, MemoryVote: &tx.MemoryVote{
		MemoryID: memoryID, Decision: tx.VoteDecisionAccept,
	}}
	rawFinalVote := signedV20Tx(t, finalVote, validators[2])
	finalVoteBlock := &abcitypes.RequestFinalizeBlock{
		Height: 5, Time: time.Unix(3_005, 0).UTC(), Txs: [][]byte{rawFinalVote},
	}
	app.offchainStore = &busyInjectingStore{OffchainStore: sqliteStore, alwaysFail: true}
	app.flushMaxRetries = 1
	firstFinalVoteResp, err := app.FinalizeBlock(context.Background(), finalVoteBlock)
	require.NoError(t, err)
	require.Zero(t, firstFinalVoteResp.TxResults[0].Code, firstFinalVoteResp.TxResults[0].Log)
	requireCommitPanicV20(t, app)
	require.NoError(t, app.Close())
	currentApp = nil

	app, sqliteStore = openRecoveredApp()
	currentApp = app
	assert.Equal(t, int64(4), app.state.Height)
	rebuilt, err = app.RebuildScopedProjection(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, rebuilt)
	projected, err = sqliteStore.GetMemory(context.Background(), memoryID)
	require.NoError(t, err)
	assert.Equal(t, memory.StatusCommitted, projected.Status)
	replayedFinalVoteResp := finalizeAndCommitV20Block(t, app, finalVoteBlock)
	require.Zero(t, replayedFinalVoteResp.TxResults[0].Code, replayedFinalVoteResp.TxResults[0].Log)
	assert.Equal(t, firstFinalVoteResp.AppHash, replayedFinalVoteResp.AppHash)
	votes, err := sqliteStore.GetVotes(context.Background(), memoryID)
	require.NoError(t, err)
	assert.Len(t, votes, 3)
	persistedState, err := LoadState(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, int64(5), persistedState.Height)

	lateReplay := app.processTx(finalVote, 6, time.Unix(3_006, 0).UTC())
	assert.Equal(t, uint32(4), lateReplay.Code, "height-bound recovery must not weaken ordinary nonce replay protection")
}

func requireCommitPanicV20(t *testing.T, app *SageApp) {
	t.Helper()
	defer func() {
		require.NotNil(t, recover(), "Commit must halt when the projection flush fails")
	}()
	_, _ = app.Commit(context.Background(), &abcitypes.RequestCommit{})
}

func seedV20SnapshotFilesystem(t *testing.T, dataDir string) {
	t.Helper()
	cometData := filepath.Join(dataDir, "cometbft", "data", "blockstore.db")
	require.NoError(t, os.MkdirAll(cometData, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(cometData, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600))
	configDir := filepath.Join(dataDir, "cometbft", "config")
	require.NoError(t, os.MkdirAll(configDir, 0o700))
	for _, name := range []string{"genesis.json", "node_key.json", "priv_validator_key.json"} {
		require.NoError(t, os.WriteFile(filepath.Join(configDir, name), []byte(`{"fixture":true}`), 0o600))
	}
}
