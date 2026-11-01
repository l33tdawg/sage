package abci

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

	abcitypes "github.com/cometbft/cometbft/abci/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/consensuskeys"
	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

type freshV30StateSyncFixture struct {
	directory      string
	projectionPath string
	backup         []byte
	appHash        []byte
	origin         *store.FreshGenesisActivation
	parent         tx.ParentRef
	child          tx.ParentRef
	grandchild     tx.ParentRef
	decisionID     string
	signerID       string
	publicMemoryID string
}

// The genesis and public memory use actual InitChain/FinalizeBlock/Commit.
// The lineage/taint/provenance corpus is a store fixture for recovery validation;
// this test does not claim to exercise governance voting or network transport.
func newFreshV30StateSyncFixture(t *testing.T) freshV30StateSyncFixture {
	t.Helper()
	ctx := context.Background()
	directory := t.TempDir()
	fixture := freshV30StateSyncFixture{
		directory: filepath.Join(directory, "source"), projectionPath: filepath.Join(directory, "source.db"),
		decisionID: strings.Repeat("a", 64),
	}
	app := openFreshV30StateSyncApp(t, fixture.directory, fixture.projectionPath)
	t.Cleanup(func() {
		if app != nil {
			require.NoError(t, app.Close())
		}
	})
	manifest, root, agent := freshGenesisFixture(t, 30, 4, true)
	const chainID = "fresh-v30-state-sync"
	_, err := app.InitChain(ctx, freshGenesisRequest(t, chainID, manifest, root, agent))
	require.NoError(t, err)
	fixture.origin, err = app.badgerStore.GetFreshGenesisActivation()
	require.NoError(t, err)
	fixture.signerID = agent.id

	submit := makeMemorySubmitTx(t, *agent, "fresh-home", "public fresh-genesis state-sync memory")
	raw := encodeDirectH0Tx(t, submit, *agent, 1)
	finalized, err := app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{Height: 1, Time: time.Now().UTC(), Txs: [][]byte{raw}})
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, 1)
	require.Zero(t, finalized.TxResults[0].Code, finalized.TxResults[0].Log)
	fixture.publicMemoryID = string(finalized.TxResults[0].Data)
	_, err = app.Commit(ctx, &abcitypes.RequestCommit{})
	require.NoError(t, err)
	_, err = app.badgerStore.PublicMemoryBranch(fixture.publicMemoryID)
	require.NoError(t, err, "block1 must update the fresh origin's public-memory tree")

	fixture.parent = tx.ParentRef{SourceChainID: chainID, MemoryID: "fresh-sync-parent", ContentHash: sha256.Sum256([]byte("fresh sync parent"))}
	fixture.child = tx.ParentRef{SourceChainID: chainID, MemoryID: "fresh-sync-child", ContentHash: sha256.Sum256([]byte("fresh sync child"))}
	fixture.grandchild = tx.ParentRef{SourceChainID: chainID, MemoryID: "fresh-sync-grandchild", ContentHash: sha256.Sum256([]byte("fresh sync grandchild"))}
	for _, ref := range []tx.ParentRef{fixture.parent, fixture.child, fixture.grandchild} {
		require.NoError(t, app.badgerStore.SetMemoryHash(ref.MemoryID, ref.ContentHash[:], "committed"))
		require.NoError(t, app.badgerStore.SetMemoryClassification(ref.MemoryID, uint8(store.ClearanceInternal)))
		require.NoError(t, app.badgerStore.SetMemoryDomain(ref.MemoryID, "fresh-home"))
		require.NoError(t, app.badgerStore.SetMemoryAuthor(ref.MemoryID, agent.id))
		require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(ref.MemoryID, agent.id))
		require.NoError(t, app.badgerStore.SetMemorySubmissionHeight(ref.MemoryID, 2))
	}
	require.NoError(t, app.badgerStore.SetMemoryLineage(fixture.child.MemoryID, []tx.ParentRef{fixture.parent}))
	require.NoError(t, app.badgerStore.SetMemoryLineage(fixture.grandchild.MemoryID, []tx.ParentRef{fixture.child}))
	created, err := app.badgerStore.ApplyTaintDecision(chainID, fixture.decisionID, fixture.parent, sha256.Sum256([]byte("fresh sync taint payload")))
	require.NoError(t, err)
	require.True(t, created)
	expanded, err := app.badgerStore.ExpandTaintDecision(chainID, fixture.decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, taint.StatusComplete, expanded.Status)
	require.NoError(t, app.badgerStore.SetAppV30Provenance(fixture.child.MemoryID, []store.AppV30ProvenanceSigner{{CredentialID: agent.id, PrincipalID: agent.id, Exact: true}}))
	_, err = app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{Height: 2, Time: time.Now().UTC()})
	require.NoError(t, err)
	_, err = app.Commit(ctx, &abcitypes.RequestCommit{})
	require.NoError(t, err)
	fixture.appHash = append([]byte(nil), app.state.AppHash...)
	require.NoError(t, app.Close())
	app = nil

	// Reopen the actual durable files, rather than constructing a new app over
	// the existing open store objects.
	app = openFreshV30StateSyncApp(t, fixture.directory, fixture.projectionPath)
	verifyFreshV30StateSyncFixture(t, app, fixture)
	var backup bytes.Buffer
	require.NoError(t, statesync.WriteCanonicalState(ctx, app.badgerStore.DB(), &backup))
	fixture.backup = append([]byte(nil), backup.Bytes()...)
	require.NoError(t, app.Close())
	app = nil
	return fixture
}

func openFreshV30StateSyncApp(t *testing.T, directory, projectionPath string) *SageApp {
	t.Helper()
	canonical, err := store.NewBadgerStore(directory)
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), projectionPath)
	if err != nil {
		_ = canonical.CloseBadger()
		require.NoError(t, err)
	}
	app, err := NewSageAppWithStores(canonical, projection, zerolog.Nop())
	if err != nil {
		_ = projection.Close()
		_ = canonical.CloseBadger()
		require.NoError(t, err)
	}
	return app
}

func verifyFreshV30StateSyncFixture(t *testing.T, app *SageApp, fixture freshV30StateSyncFixture) {
	t.Helper()
	info, err := app.Info(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), info.AppVersion)
	require.Equal(t, int64(2), info.LastBlockHeight)
	require.Equal(t, fixture.appHash, info.LastBlockAppHash)
	require.True(t, app.IsAppV30ActiveForNextTx())
	require.Len(t, app.ValidatorIDs(), 4)
	activation, err := app.badgerStore.GetFreshGenesisActivation()
	require.NoError(t, err)
	require.Equal(t, fixture.origin, activation)
	for version := uint64(2); version <= 30; version++ {
		record, err := app.badgerStore.GetAppliedUpgrade(fmt.Sprintf("app-v%d", version))
		require.NoError(t, err)
		require.Nil(t, record, "restoration must not invent an upgrade ladder")
	}
	require.NoError(t, app.badgerStore.ValidateFreshGenesisLineage())
	require.NoError(t, app.badgerStore.ValidateLineageIndexes())
	require.NoError(t, app.badgerStore.ValidateTaintState())
	for _, ref := range []tx.ParentRef{fixture.parent, fixture.child, fixture.grandchild} {
		decisions, err := app.badgerStore.GetEffectiveTaintDecisions(ref.MemoryID)
		require.NoError(t, err)
		require.Equal(t, []string{fixture.decisionID}, decisions)
	}
	parents, err := app.badgerStore.GetMemoryLineage(fixture.grandchild.MemoryID)
	require.NoError(t, err)
	require.Equal(t, []tx.ParentRef{fixture.child}, parents)
	signers, found, err := app.badgerStore.GetAppV30Provenance(fixture.child.MemoryID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []store.AppV30ProvenanceSigner{{CredentialID: fixture.signerID, PrincipalID: fixture.signerID, Exact: true}}, signers)
	branch, err := app.badgerStore.PublicMemoryBranch(fixture.publicMemoryID)
	require.NoError(t, err)
	require.NoError(t, store.VerifyPublicMemoryBranch(*branch, branch.Root))
	computed, err := app.badgerStore.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	require.Equal(t, fixture.appHash, computed)
	narrow, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.NotEqual(t, fixture.appHash, narrow, "fresh v30 uses the composite rule")
}

func TestFreshV30DurableCanonicalStateSyncRoundTrip(t *testing.T) {
	fixture := newFreshV30StateSyncFixture(t)
	ctx := context.Background()
	height, appHash, err := InspectStateSyncRecoveryDirectory(ctx, fixture.directory)
	require.NoError(t, err)
	require.Equal(t, uint64(2), height)
	require.Equal(t, fixture.appHash, appHash)
	directory := t.TempDir()
	backupPath := filepath.Join(directory, "fresh-v30.backup")
	require.NoError(t, os.WriteFile(backupPath, fixture.backup, 0o600))
	target := filepath.Join(directory, "restored")
	require.NoError(t, PrepareAppV20StateSyncBackup(ctx, backupPath, target, 2, fixture.appHash))
	require.NoError(t, VerifyActivatedAppV20StateSyncDirectory(ctx, target, 2, fixture.appHash))
	app := openFreshV30StateSyncApp(t, target, filepath.Join(directory, "restored.db"))
	defer func() { require.NoError(t, app.Close()) }()
	verifyFreshV30StateSyncFixture(t, app, fixture)
	_, err = app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{Height: 3, Time: time.Now().UTC()})
	require.NoError(t, err)
	_, err = app.Commit(ctx, &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, int64(3), app.state.Height)
	require.Equal(t, uint64(30), app.currentAppVersion())
}

func TestFreshV30RecoveryInspectorRejectsCorruptQuarantine(t *testing.T) {
	fixture := newFreshV30StateSyncFixture(t)
	for _, testCase := range []struct {
		name      string
		want      string
		recompute bool
		mutate    func(*testing.T, *store.BadgerStore)
	}{
		{name: "malformed origin", want: "fresh genesis", mutate: func(t *testing.T, s *store.BadgerStore) {
			require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(consensuskeys.FreshGenesisActivation), []byte(`{}`))
			}))
		}},
		{name: "changed origin", want: "persisted AppHash", mutate: func(t *testing.T, s *store.BadgerStore) {
			origin, err := s.GetFreshGenesisActivation()
			require.NoError(t, err)
			origin.AppVersion = 29
			encoded, err := json.Marshal(origin)
			require.NoError(t, err)
			require.NoError(t, s.DB().Update(func(txn *badger.Txn) error { return txn.Set([]byte(consensuskeys.FreshGenesisActivation), encoded) }))
		}},
		{name: "changed stored hash", want: "persisted AppHash", mutate: func(t *testing.T, s *store.BadgerStore) {
			require.NoError(t, s.SetState(stateAppHashKey, bytes.Repeat([]byte{0x81}, sha256.Size)))
		}},
		{name: "synthetic history", want: "applied app-v29", recompute: true, mutate: func(t *testing.T, s *store.BadgerStore) {
			require.NoError(t, s.MarkUpgradeApplied(appV29UpgradeName, 29, 1))
		}},
		{name: "missing taint count", want: "active count", recompute: true, mutate: func(t *testing.T, s *store.BadgerStore) {
			require.NoError(t, s.DB().Update(func(txn *badger.Txn) error { return txn.Delete(taint.ActiveCountKey()) }))
		}},
		{name: "missing reverse lineage", want: "missing reverse edge", recompute: true, mutate: func(t *testing.T, s *store.BadgerStore) {
			parentID, err := tx.ParentRefIDHex(fixture.parent)
			require.NoError(t, err)
			require.NoError(t, s.DB().Update(func(txn *badger.Txn) error { return txn.Delete(lineage.ReverseKey(parentID, fixture.child.MemoryID)) }))
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "quarantine")
			db, err := badger.Open(badger.DefaultOptions(directory).WithLogger(nil))
			require.NoError(t, err)
			require.NoError(t, statesync.RestoreCanonicalState(context.Background(), bytes.NewReader(fixture.backup), db))
			require.NoError(t, db.Close())
			canonical, err := store.OpenBadgerStoreWithoutMigrations(directory)
			require.NoError(t, err)
			testCase.mutate(t, canonical)
			if testCase.recompute {
				state, err := LoadState(canonical)
				require.NoError(t, err)
				state.AppHash, err = canonical.ComputePublicMemoryAppHash()
				require.NoError(t, err)
				require.NoError(t, SaveState(canonical, state))
			}
			require.NoError(t, canonical.CloseBadger())
			_, _, err = InspectStateSyncRecoveryDirectory(context.Background(), directory)
			require.ErrorContains(t, err, testCase.want)
		})
	}
}
