package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"path/filepath"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/poe"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

type appV30ConstructorFixture struct {
	path       string
	badger     *store.BadgerStore
	projection *store.SQLiteStore
}

// Seed real persisted prerequisite records, not SageApp's cached fork fields.
func newAppV30ConstructorFixture(t *testing.T, active bool) *appV30ConstructorFixture {
	t.Helper()
	directory := t.TempDir()
	path := filepath.Join(directory, "badger")
	bs, _ := seedDirectAppV25HomeRepairFixture(t, path)
	projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(directory, "projection.db"))
	require.NoError(t, err)
	f := &appV30ConstructorFixture{path: path, badger: bs, projection: projection}
	t.Cleanup(func() {
		require.NoError(t, f.badger.CloseBadger())
		require.NoError(t, f.projection.Close())
	})
	require.NoError(t, bs.MigrateAppV26AccessGroupAuthorities(4))
	require.NoError(t, bs.MarkUpgradeApplied(appV26UpgradeName, 26, 4))
	require.NoError(t, bs.MarkUpgradeApplied(appV27UpgradeName, 27, 5))
	previous, err := bs.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	// Existing v28 prerequisite preparation, not a new admission/producer path.
	require.NoError(t, bs.PreparePublicMemoryStage(context.Background(), 6))
	require.NoError(t, bs.PrepareCoCommitTombstoneStage(context.Background(), 6))
	scoped := bs.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.PromotePublicMemoryStage(6, previous))
	require.NoError(t, scoped.PromoteCoCommitTombstoneStage(6, previous))
	require.NoError(t, scoped.MarkUpgradeApplied(appV28UpgradeName, 28, 6))
	require.NoError(t, scoped.MarkUpgradeApplied(appV29UpgradeName, 29, 7))
	if active {
		require.NoError(t, scoped.MarkUpgradeApplied(appV30UpgradeName, 30, 8))
	}
	require.NoError(t, scoped.CommitConsensusTransaction())
	return f
}

func (f *appV30ConstructorFixture) persistAndReopen(t *testing.T) []byte {
	t.Helper()
	state := &AppState{Height: 9, EpochNum: poe.EpochNumber(9)}
	for range 2 {
		hash, err := f.badger.ComputePublicMemoryAppHash()
		require.NoError(t, err)
		state.AppHash = bytes.Clone(hash)
		require.NoError(t, SaveState(f.badger, state))
	}
	require.NoError(t, f.badger.CloseBadger())
	var err error
	f.badger, err = store.NewBadgerStore(f.path)
	require.NoError(t, err)
	return state.AppHash
}

func appV30ConstructorTarget() tx.ParentRef {
	return tx.ParentRef{SourceChainID: "sage", MemoryID: "recovery-target", ContentHash: sha256.Sum256([]byte("recovery-target-content"))}
}

func seedAppV30ConstructorDecision(t *testing.T, f *appV30ConstructorFixture) (string, tx.ParentRef) {
	t.Helper()
	id := strings.Repeat("a", taint.DecisionIDBytes)
	target := appV30ConstructorTarget()
	created, err := f.badger.ApplyTaintDecision("sage", id, target, sha256.Sum256([]byte("constructor-payload")))
	require.NoError(t, err)
	require.True(t, created)
	return id, target
}

func appV30ConstructorValue(t *testing.T, bs *store.BadgerStore, key []byte) ([]byte, bool) {
	t.Helper()
	var value []byte
	found := false
	require.NoError(t, bs.DB().View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		found = true
		return err
	}))
	return value, found
}

func requireAppV30CallerStoresUsable(t *testing.T, f *appV30ConstructorFixture) {
	t.Helper()
	require.NoError(t, f.projection.Ping(context.Background()))
	require.NoError(t, f.badger.SetState("constructor-caller-probe", []byte("open")))
	value, err := f.badger.GetState("constructor-caller-probe")
	require.NoError(t, err)
	require.Equal(t, []byte("open"), value)
}

func TestAppV30ConstructorRecoveryValidActiveReopen(t *testing.T) {
	f := newAppV30ConstructorFixture(t, true)
	seedAppV30ConstructorDecision(t, f)
	wantHash := f.persistAndReopen(t)
	app, err := NewSageAppWithStores(f.badger, f.projection, zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, int64(8), app.appV30AppliedHeight)
	require.Equal(t, int64(7), app.appV29AppliedHeight)
	info, err := app.Info(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), info.AppVersion)
	require.Equal(t, int64(9), info.LastBlockHeight)
	require.Equal(t, wantHash, info.LastBlockAppHash)
	actual, err := f.badger.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	require.Equal(t, wantHash, actual)
}

func TestAppV30ConstructorRecoveryRejectsPersistedTargetDamage(t *testing.T) {
	for _, damage := range []string{"queued pair loss", "completed pair loss", "missing anchor", "malformed anchor", "orphan anchor"} {
		t.Run(damage, func(t *testing.T) {
			f := newAppV30ConstructorFixture(t, true)
			id, target := seedAppV30ConstructorDecision(t, f)
			if damage == "completed pair loss" {
				result, err := f.badger.ExpandTaintDecision("sage", id, taint.PerExecutionUnitBudget)
				require.NoError(t, err)
				require.Equal(t, taint.StatusComplete, result.Status)
				_, found := appV30ConstructorValue(t, f.badger, taint.FrontierKey(id, 1, 0))
				require.False(t, found)
			}
			require.NoError(t, f.badger.DB().Update(func(txn *badger.Txn) error {
				switch damage {
				case "queued pair loss", "completed pair loss":
					if err := txn.Delete(taint.MembershipKey(target.MemoryID, id)); err != nil {
						return err
					}
					return txn.Delete(taint.DecisionIndexKey(id, target.MemoryID))
				case "missing anchor":
					return txn.Delete(taint.TargetKey(id))
				case "malformed anchor":
					return txn.Set(taint.TargetKey(id), []byte{2})
				default:
					return txn.Delete(taint.DecisionKey(id))
				}
			}))
			f.persistAndReopen(t)
			app, err := NewSageAppWithStores(f.badger, f.projection, zerolog.Nop())
			require.Nil(t, app)
			require.ErrorIs(t, err, store.ErrTaintStateMalformed)
			require.ErrorContains(t, err, "validate app-v30 taint state")
			requireAppV30CallerStoresUsable(t, f)
		})
	}
}

func TestAppV30ConstructorRecoveryRetainsCleaningAndCleanAnchor(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleaning", true: "clean"}[clean], func(t *testing.T) {
			f := newAppV30ConstructorFixture(t, true)
			id, _ := seedAppV30ConstructorDecision(t, f)
			anchor, found := appV30ConstructorValue(t, f.badger, taint.TargetKey(id))
			require.True(t, found)
			_, err := f.badger.ClearTaintDecision(id)
			require.NoError(t, err)
			if clean {
				status := taint.StatusCleaning
				for range 4 {
					result, err := f.badger.CleanupTaintDecision(id, taint.PerExecutionUnitBudget)
					require.NoError(t, err)
					status = result.Status
					if status == taint.StatusClean {
						break
					}
				}
				require.Equal(t, taint.StatusClean, status)
			}
			f.persistAndReopen(t)
			app, err := NewSageAppWithStores(f.badger, f.projection, zerolog.Nop())
			require.NoError(t, err)
			require.Equal(t, uint64(30), app.currentAppVersion())
			retained, found := appV30ConstructorValue(t, f.badger, taint.TargetKey(id))
			require.True(t, found)
			require.Equal(t, anchor, retained)
		})
	}
}

func snapshotAppV30ConstructorDB(t *testing.T, bs *store.BadgerStore) map[string][]byte {
	t.Helper()
	values := make(map[string][]byte)
	require.NoError(t, bs.DB().View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			values[string(it.Item().KeyCopy(nil))] = value
		}
		return nil
	}))
	return values
}

func TestAppV30ConstructorRecoveryDormantDoesNotWrite(t *testing.T) {
	f := newAppV30ConstructorFixture(t, false)
	f.persistAndReopen(t)
	// Normalize any pre-existing constructor bookkeeping before the audit probe.
	_, err := NewSageAppWithStores(f.badger, f.projection, zerolog.Nop())
	require.NoError(t, err)
	id, target := seedAppV30ConstructorDecision(t, f)
	require.NoError(t, f.badger.SetMemoryLineage("child", []tx.ParentRef{target}))
	parentID, err := tx.ParentRefIDHex(target)
	require.NoError(t, err)
	require.NoError(t, f.badger.DB().Update(func(txn *badger.Txn) error {
		if err := txn.Delete(lineage.ReverseKey(parentID, "child")); err != nil {
			return err
		}
		return txn.Delete(taint.TargetKey(id))
	}))
	f.persistAndReopen(t)
	before := snapshotAppV30ConstructorDB(t, f.badger)
	app, err := NewSageAppWithStores(f.badger, f.projection, zerolog.Nop())
	require.NoError(t, err, "dormant app30 must not invoke graph recovery")
	require.Equal(t, int64(0), app.appV30AppliedHeight)
	require.Equal(t, uint64(29), app.currentAppVersion())
	require.Equal(t, before, snapshotAppV30ConstructorDB(t, f.badger))
}

func TestAppV30ConstructorRecoveryRepairPersistsBeforeLaterRefusal(t *testing.T) {
	f := newAppV30ConstructorFixture(t, true)
	id, target := seedAppV30ConstructorDecision(t, f)
	require.NoError(t, f.badger.SetMemoryLineage("child", []tx.ParentRef{target}))
	parentID, err := tx.ParentRefIDHex(target)
	require.NoError(t, err)
	reverse := lineage.ReverseKey(parentID, "child")
	require.NoError(t, f.badger.DB().Update(func(txn *badger.Txn) error {
		if err := txn.Delete(reverse); err != nil {
			return err
		}
		return txn.Delete(taint.DecisionIndexKey(id, target.MemoryID))
	}))
	f.persistAndReopen(t)
	_, found := appV30ConstructorValue(t, f.badger, reverse)
	require.False(t, found)
	app, err := NewSageAppWithStores(f.badger, f.projection, zerolog.Nop())
	require.Nil(t, app)
	require.ErrorIs(t, err, store.ErrTaintStateMalformed)
	require.ErrorContains(t, err, "validate app-v30 taint state")
	// Observe durability of the successful earlier repair, without promising rollback.
	require.NoError(t, f.badger.CloseBadger())
	f.badger, err = store.NewBadgerStore(f.path)
	require.NoError(t, err)
	value, found := appV30ConstructorValue(t, f.badger, reverse)
	require.True(t, found)
	require.Empty(t, value)
	requireAppV30CallerStoresUsable(t, f)
}
