package abci

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/validator"
)

// v8.2 validator lifecycle: processEpoch persists the normalized weight set;
// a fresh SageApp on the same store hydrates PoEWeight from poew:<id>.
//
//   L1: validator added mid-epoch is written to poew:<id> at the next boundary
//       and survives a process restart.
//   L2: validator removed before the next epoch is pruned from poew:* (no
//       stale weight applies to a successor at the same ID).
//   L3: node restart between epoch boundaries — first epoch's PoEWeight
//       persists through close+reopen, no re-running processEpoch.

// newAppOnStores returns a SageApp that already has a v8.2 fork-gate set so
// processEpoch will persist post-fork. Uses the same badger + sqlite paths so
// subsequent close/reopen pairs see the same on-chain state.
func newAppOnStores(t *testing.T, badgerDir, sqlitePath string) *SageApp {
	t.Helper()
	bs, err := store.NewBadgerStore(badgerDir)
	require.NoError(t, err)
	t.Cleanup(func() { bs.CloseBadger() })

	sqlite, err := store.NewSQLiteStore(context.Background(), sqlitePath)
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })

	app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
	require.NoError(t, err)
	return app
}

// activateFork marks the v8.2 upgrade applied in the audit trail so any
// subsequent restart hydrates v8_2AppliedHeight via refreshV8_2Fork. Also
// sets the in-memory cache for the current process.
func activateFork(t *testing.T, app *SageApp, height int64) {
	t.Helper()
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(v8_2UpgradeName, 3, height))
	app.v8_2AppliedHeight = height
}

// L1: validator added mid-epoch, processEpoch at the next boundary writes
// poew:<id>, a fresh app on the same store hydrates PoEWeight on boot.
func TestValidatorLifecycle_L1_PersistAndHydrate(t *testing.T) {
	tmp := t.TempDir()
	badgerDir := filepath.Join(tmp, "badger")
	sqlitePath := filepath.Join(tmp, "off.db")

	app := newAppOnStores(t, badgerDir, sqlitePath)
	activateFork(t, app, 50)

	// Four validators with vote stats that yield distinct accuracy values.
	// processEpoch combines accuracy (cold-start blended) with a default
	// domain/recency/corroboration mix; we don't pin the exact weight values,
	// only that they persist and survive a restart.
	for _, vid := range []string{"v-l1-0", "v-l1-1", "v-l1-2", "v-l1-3"} {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID:    vid,
			Power: 1,
		}))
		require.NoError(t, app.badgerStore.IncrementVoteStats(vid, true, 90))
		require.NoError(t, app.badgerStore.IncrementVoteStats(vid, true, 95))
	}

	// Run processEpoch at the first post-fork boundary (H=100, gate at 50).
	app.processEpoch(100, time.Unix(2000, 0))

	// poew:current must exist; weights map must contain all four validators.
	weights, ok, err := app.badgerStore.GetEpochWeights()
	require.NoError(t, err)
	require.True(t, ok, "post-fork processEpoch must persist poew:current")
	require.Len(t, weights, 4)

	// Snapshot the persisted weights — these are the ground truth a fresh
	// process must rebuild on boot.
	want := make(map[string]float64, len(weights))
	for k, v := range weights {
		want[k] = v
		require.Greater(t, v, 0.0, "%s should have a positive PoE weight after epoch run", k)
	}

	// Close the first app's badger store so the fresh process can open it.
	require.NoError(t, app.badgerStore.CloseBadger())
	require.NoError(t, app.offchainStore.Close())

	// Re-create the validators on a fresh app — refreshPoEWeights pulls the
	// PoEWeight back out of BadgerDB and onto each in-memory ValidatorInfo.
	// (NewSageAppWithStores' LoadValidators path only restores ID + Power,
	// not PoEWeight, so the hydration step is what closes the gap.)
	app2 := newAppOnStores(t, badgerDir, sqlitePath)
	for _, vid := range []string{"v-l1-0", "v-l1-1", "v-l1-2", "v-l1-3"} {
		require.NoError(t, app2.validators.AddValidator(&validator.ValidatorInfo{
			ID:    vid,
			Power: 1,
		}))
	}
	app2.refreshPoEWeights()

	for _, v := range app2.validators.GetAll() {
		assert.InDelta(t, want[v.ID], v.PoEWeight, 1e-12,
			"%s PoEWeight must round-trip through close+reopen", v.ID)
	}
	assert.Equal(t, int64(50), app2.v8_2AppliedHeight,
		"refreshV8_2Fork must rehydrate the fork height on boot")
}

// L2: a validator removed before the next epoch boundary is pruned from
// poew:*. Exercises the end-to-end abci path on top of badger.go's W3
// store-side pruning.
func TestValidatorLifecycle_L2_RemovalPrunesPoEW(t *testing.T) {
	tmp := t.TempDir()
	badgerDir := filepath.Join(tmp, "badger")
	sqlitePath := filepath.Join(tmp, "off.db")

	app := newAppOnStores(t, badgerDir, sqlitePath)
	activateFork(t, app, 50)

	for _, vid := range []string{"v-l2-0", "v-l2-1", "v-l2-2", "v-l2-3"} {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID:    vid,
			Power: 1,
		}))
		require.NoError(t, app.badgerStore.IncrementVoteStats(vid, true, 90))
	}

	// First post-fork epoch — all four get poew:<id>.
	app.processEpoch(100, time.Unix(2000, 0))
	w1, ok, err := app.badgerStore.GetEpochWeights()
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, w1, 4)

	// Remove one validator (simulates governance dropping them from the set).
	require.NoError(t, app.validators.RemoveValidator("v-l2-2"))

	// Second post-fork epoch — the dropped validator must not appear.
	app.processEpoch(200, time.Unix(2100, 0))
	w2, ok, err := app.badgerStore.GetEpochWeights()
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, w2, 3, "dropped validator must be pruned from poew:*")
	_, present := w2["v-l2-2"]
	assert.False(t, present, "v-l2-2 must not have a poew:<id> entry after removal")
}

// L3: node restart between epoch boundaries — first epoch's weights survive
// close+reopen and are restored without re-running processEpoch. Closes the
// in-memory-only hazard from the plan.
func TestValidatorLifecycle_L3_RestartBetweenEpochs(t *testing.T) {
	tmp := t.TempDir()
	badgerDir := filepath.Join(tmp, "badger")
	sqlitePath := filepath.Join(tmp, "off.db")

	app := newAppOnStores(t, badgerDir, sqlitePath)
	activateFork(t, app, 50)

	ids := []string{"v-l3-0", "v-l3-1", "v-l3-2", "v-l3-3"}
	for _, vid := range ids {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID:    vid,
			Power: 1,
		}))
		require.NoError(t, app.badgerStore.IncrementVoteStats(vid, true, 90))
	}

	// One post-fork epoch ran at H=100.
	app.processEpoch(100, time.Unix(2000, 0))
	w1, ok, err := app.badgerStore.GetEpochWeights()
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, w1)

	// Simulate "node dies at H=150" — between epoch boundaries.
	require.NoError(t, app.badgerStore.CloseBadger())
	require.NoError(t, app.offchainStore.Close())

	// Restart. The validator set is re-added with PoEWeight=0; refreshPoEWeights
	// brings the persisted values back.
	app2 := newAppOnStores(t, badgerDir, sqlitePath)
	for _, vid := range ids {
		require.NoError(t, app2.validators.AddValidator(&validator.ValidatorInfo{
			ID:    vid,
			Power: 1,
		}))
	}
	app2.refreshPoEWeights()

	for _, v := range app2.validators.GetAll() {
		assert.InDelta(t, w1[v.ID], v.PoEWeight, 1e-12,
			"%s PoEWeight at H=150 must equal the epoch-1 boundary value", v.ID)
	}
}
